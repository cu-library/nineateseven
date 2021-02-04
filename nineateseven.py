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
@click.option("--fieldmapping", "-fm", type=click.File(), required=True)
@click.argument("bundles", nargs=-1)
def cli(
    db,
    dbcharset,
    dbusername,
    dbpassword,
    target,
    targetusername,
    targetpassword,
    fieldmapping,
    bundles,
):
    """Migrate a Drupal 7 content type to Drupal 9."""
    # Connect to the Drupal 7 database
    click.echo("Connecting to Drupal 7 database...")
    click.echo(f"    Database name: {db}")
    click.echo(f" Database charset: {dbcharset}")
    click.echo(f"    Database user: {dbusername}")

    connection = pymysql.connect(
        host="localhost",
        database=db,
        charset=dbcharset,
        user=dbusername,
        password=dbpassword,
        cursorclass=pymysql.cursors.DictCursor,
    )

    config = configparser.ConfigParser()
    config.read_file(fieldmapping)

    with connection:
        for bundle in bundles:
            count = count_nodes_of_bundle(connection, bundle)
            click.echo(f"Count: {count}")
            fields = fields_of_bundle(connection, bundle)
            click.echo(fields)
            nodes = create_nodes(connection, bundle)
            for d7fieldname, d9fieldname in config["fieldmapping"]:
                field_config = fields[d7fieldname]
                add_field(nodes, connection, bundle, d7fieldname, d9fieldname, field_config)
            for node in nodes:
                del node["nid"]
                post(node, bundle, target, targetusername, targetpassword)


def count_nodes_of_bundle(connection, bundle):
    with connection.cursor() as cursor:
        sql = "SELECT COUNT(*) AS count FROM `node` WHERE `type`=%s"
        cursor.execute(sql, (bundle,))
        result = cursor.fetchone()
        return result["count"]


def fields_of_bundle(connection, bundle):
    field_ids = []
    with connection.cursor() as cursor:
        sql = "SELECT * FROM `field_config_instance` WHERE entity_type='node' AND bundle=%s"
        cursor.execute(sql, (bundle,))
        for row in cursor:
            fields.append(row["field_id"])
    fields = {}
    for field_id in field_ids:
        with connection.cursor() as cursor:
            sql = "SELECT * FROM `field_config` WHERE id=%s"
            cursor.execute(sql, (field_id,))
            row = cursor.fetchone()
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
            node["data"]["attributes"]["title"] = row["title"]
            node["data"]["attributes"]["status"] = row["status"] == 1
            node["data"]["attributes"]["promote"] = row["promote"] == 1
            node["data"]["attributes"]["sticky"] = row["sticky"] == 1
            node["data"]["attributes"]["created"] = datetime.datetime.fromtimestamp(
                row["created"], tz=datetime.timezone.utc
            ).isoformat()
            # node["data"]["attributes"]["changed"] = datetime.datetime.fromtimestamp(
            #    row["changed"], tz=datetime.timezone.utc
            # ).isoformat()
            nodes.append(node)
    return nodes


def add_field(nodes, connection, bundle, d7fieldname, d9fieldname, field_config):
    for node in nodes:
        with connection.cursor() as cursor:
            sql = f"SELECT * FROM `field_data_{d7fieldname}` WHERE `entity_type`='node' AND bundle=%s AND entity_id=%s"
            cursor.execute(sql, (bundle, node["nid"]))
            fields_data = []
            for row in cursor:
                field_data = {}
                if field_config["type"] == "link_field":
                    field_data["uri"] = row["field_database_link_url"]
                    field_data["title"] = row["field_database_link_title"]
                fields_data.append(field_data)
            if fields_data:
                node["data"]["attributes"][d9fieldname] = fields_data


def post(node, bundle, target, targetusername, targetpassword):
    url = target + "/" + bundle
    headers = {
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }
    click.echo(url)
    resp = requests.post(
        url,
        headers=headers,
        data=json.dumps(node),
        auth=(targetusername, targetpassword),
    )
    click.echo(resp)
    click.echo(pprint.pprint(resp.json()))
