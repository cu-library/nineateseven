# -*- coding: utf-8 -*-
"""A CLI tool for migrating our Drupal 7 site to Drupal 9"""

import click
import pymysql
import datetime
import json
import requests
import pprint
import configparser

CONTEXT_SETTINGS = {"auto_envvar_prefix": "NINEATESEVEN"}


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--db", default="drupal", show_default=True)
@click.option("--dbcharset", default="utf8mb4", show_default=True)
@click.option("--dbusername", "-dbu", default="drupal", show_default=True)
@click.option("--dbpassword", "-dbp", required=True)
@click.option("--target", "-t", required=True)
@click.option("--targetusername", "-tu", required=True)
@click.option("--targetpassword", "-tp", required=True)
@click.option("--fieldmappings", "-fm", type=click.File(), required=True)
@click.option(
    "--disablesubjectaltnamewarning/--no-disablesubjectaltnamewarning", default=False
)
@click.argument("bundles", nargs=-1)
def cli(
    db,
    dbcharset,
    dbusername,
    dbpassword,
    target,
    targetusername,
    targetpassword,
    fieldmappings,
    disablesubjectaltnamewarning,
    bundles,
):
    """Migrate a Drupal 7 node bundle to Drupal 9."""
    click.echo("Starting migration...")
    click.echo(f"    Database name: {db}")
    click.echo(f" Database charset: {dbcharset}")
    click.echo(f"    Database user: {dbusername}")
    click.echo(f"  Target JSON API: {target}")
    click.echo("")

    # Connect to the local MySQL database
    connection = pymysql.connect(
        host="localhost",
        database=db,
        charset=dbcharset,
        user=dbusername,
        password=dbpassword,
        cursorclass=pymysql.cursors.DictCursor,
    )

    # Pesky security warning particular to Carleton's environment
    if disablesubjectaltnamewarning:
        import warnings
        import urllib3

        warnings.simplefilter("ignore", urllib3.exceptions.SubjectAltNameWarning)

    # Read in the config file
    config = configparser.ConfigParser()
    config.read_file(fieldmappings)

    # Global nid to d9node map
    nid_to_d9node = {}

    # All fields in D7 DB
    fields = get_fields(connection)

    with connection:
        for bundle in bundles:
            count = count_nodes_of_bundle(connection, bundle)
            click.echo(f"{count} nodes of type {bundle}")
            nodes = create_nodes(connection, bundle)
            with click.progressbar(nodes) as bar:
                for node in bar:
                    nid = node["nid"]
                    del node["nid"]
                    nid_to_d9node[nid] = post_node(
                        node, bundle, target, targetusername, targetpassword
                    )

        for d7fieldname, d9fieldname in config.items("fieldmappings"):
            click.echo(f"PATCHing {d7fieldname}")
            with click.progressbar(length=len(nid_to_d9node)) as bar:
                for nid, d9node in nid_to_d9node.items():
                    field_config = fields[d7fieldname]
                    field = create_field(
                        connection,
                        bundle,
                        nid,
                        d9node["data"]["id"],
                        nid_to_d9node,
                        d7fieldname,
                        d9fieldname,
                        field_config,
                    )
                    patch_field(
                        field,
                        bundle,
                        target,
                        targetusername,
                        targetpassword,
                    )
                    bar.update(1)


def count_nodes_of_bundle(connection, bundle):
    with connection.cursor() as cursor:
        sql = "SELECT COUNT(*) AS count FROM `node` WHERE `type`=%s"
        cursor.execute(sql, (bundle,))
        result = cursor.fetchone()
        return result["count"]


def get_fields(connection):
    fields = {}
    with connection.cursor() as cursor:
        sql = "SELECT * FROM `field_config`"
        cursor.execute(sql)
        for row in cursor:
            fields[row["field_name"]] = {"type": row["type"], "module": row["module"]}
    return fields


def create_nodes(connection, bundle):
    nodes = []
    with connection.cursor() as cursor:
        sql = "SELECT * FROM `node` WHERE `type`=%s"
        cursor.execute(sql, (bundle,))
        for row in cursor:
            node = {
                "nid": row["nid"],
                "data": {"type": f"node--{bundle}", "attributes": {}},
            }
            node["data"]["attributes"]["langcode"] = "en"
            node["data"]["attributes"]["title"] = row["title"].strip()
            node["data"]["attributes"]["status"] = row["status"] == 1
            node["data"]["attributes"]["promote"] = row["promote"] == 1
            node["data"]["attributes"]["sticky"] = row["sticky"] == 1
            node["data"]["attributes"]["created"] = datetime.datetime.fromtimestamp(
                row["created"], tz=datetime.timezone.utc
            ).isoformat()
            nodes.append(node)
    return nodes


def create_field(
    connection, bundle, nid, d9id, nid_to_d9node, d7fieldname, d9fieldname, field_config
):
    field = {"data": {"type": f"node--{bundle}", "id": d9id, "attributes": {}}}
    with connection.cursor() as cursor:
        sql = (
            f"SELECT * FROM `field_data_{d7fieldname}` "
            "WHERE `entity_type`='node' AND bundle=%s AND entity_id=%s"
        )
        cursor.execute(sql, (bundle, nid))
        fields_data = []
        for row in cursor:
            field_data = {}
            if field_config["type"] == "link_field":
                try:
                    field_data["uri"] = clean_uri(
                        row[f"{d7fieldname}_url"], nid_to_d9node
                    )
                except KeyError:
                    click.echo("----")
                    click.echo(nid)
                    click.echo("----")
                field_data["title"] = row[f"{d7fieldname}_title"]
            fields_data.append(field_data)
        if fields_data:
            field["data"]["attributes"][d9fieldname] = fields_data
    return field


def clean_uri(uri, nid_to_d9node):
    if uri.startswith("node/"):
        uri = "https://library.carleton.ca/" + uri
    if uri.startswith("https://library.carleton.ca/node/"):
        nid = uri[len("https://library.carleton.ca/node/") :]
        try:
            d9node = nid_to_d9node[int(nid)]
        except KeyError:
            raise
        uri = "internal:/node/" + str(
            d9node["data"]["attributes"]["drupal_internal__nid"]
        )
    if uri.startswith("proxy.library.carleton.ca"):
        uri = "https://" + uri
    return uri


def post_node(node, bundle, target, targetusername, targetpassword):
    url = target + "/node/" + bundle
    headers = {
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }
    resp = requests.post(
        url,
        headers=headers,
        data=json.dumps(node),
        auth=(targetusername, targetpassword),
    )
    try:
        resp.raise_for_status()
    except:
        click.echo(pprint.pformat(node))
        click.echo(pprint.pformat(resp.json()))
        raise
    return resp.json()


def patch_field(field, bundle, target, targetusername, targetpassword):
    url = target + "/node/" + bundle + "/" + field["data"]["id"]
    headers = {
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }
    resp = requests.patch(
        url,
        headers=headers,
        data=json.dumps(field),
        auth=(targetusername, targetpassword),
    )
    try:
        resp.raise_for_status()
    except:
        click.echo(pprint.pformat(field))
        click.echo(pprint.pformat(resp.json()))
        raise
