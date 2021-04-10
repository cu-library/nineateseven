# -*- coding: utf-8 -*-
"""A CLI tool for migrating our Drupal 7 site to Drupal 9"""

import click
import pymysql
import datetime
import json
import requests
import pprint
import configparser
import copy
import re

CONTEXT_SETTINGS = {"auto_envvar_prefix": "NINEATESEVEN"}


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--db", default="drupal", show_default=True)
@click.option("--dbcharset", default="utf8mb4", show_default=True)
@click.option("--dbusername", "-dbu", default="drupal", show_default=True)
@click.option("--dbpassword", "-dbp", required=True)
@click.option("--target", "-t", required=True)
@click.option("--targetusername", "-tu", required=True)
@click.option("--targetpassword", "-tp", required=True)
@click.option("--config", "-c", type=click.File(), required=True)
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
    config,
    disablesubjectaltnamewarning,
    bundles,
):
    """Migrate a Drupal 7 node bundle to Drupal 9."""
    click.echo("Starting migration...")
    click.echo("")
    click.echo(f"    Database name: {db}")
    click.echo(f" Database charset: {dbcharset}")
    click.echo(f"    Database user: {dbusername}")
    click.echo(f"  Target JSON API: {target}")
    click.echo("")

    # Connect to the local MySQL database.
    connection = pymysql.connect(
        host="localhost",
        database=db,
        charset=dbcharset,
        user=dbusername,
        password=dbpassword,
        cursorclass=pymysql.cursors.DictCursor,
    )

    # Pesky security warning particular to Carleton's environment.
    if disablesubjectaltnamewarning:
        import warnings
        import urllib3

        warnings.simplefilter("ignore", urllib3.exceptions.SubjectAltNameWarning)

    # Read in the config file.
    cp = configparser.ConfigParser(interpolation=None)
    cp.read_file(config)

    # Mapping from D7 NIDs to newly created D9 nodes.
    nid_to_d9_node = {}

    # Mapping from D7 NIDs to their D9 bundle name.
    nid_to_bundle_name = {}

    # Mapping from D7 TIDs to newly created D9 taxonomy terms.
    tid_to_d9_taxonomy_term = {}

    # Context manager around the connection, so it will be automatically closed.
    with connection:
        # First, migrate the taxonomy terms.
        # We migrate them first, because nodes might reference them.
        # Iterate through the D7 machine names for taxonomy vocabularies and their
        # corresponding D9 machine names
        for d7_taxonomy_voc, d9_taxonomy_voc in cp.items("taxonomy_mappings"):
            terms = create_taxonomy_terms(connection, d7_taxonomy_voc, d9_taxonomy_voc)
            with click.progressbar(
                length=len(terms),
                label=f"POSTing taxonomy terms in {d7_taxonomy_voc}",
            ) as bar:
                # Iterate through a copy of the terms, processing terms
                # which are at the root ("0" parent) or have a parent which
                # is already processed.
                # After processing a term, delete it from the terms list.
                # Loop this process until all terms in the terms list have
                # been processed.
                while len(terms) > 0:
                    this_pass_terms = terms.copy()
                    for term in this_pass_terms:
                        if (
                            term["parent_tid"] == 0
                            or term["parent_tid"] in tid_to_d9_taxonomy_term
                        ):
                            if term["parent_tid"] in tid_to_d9_taxonomy_term:
                                term["data"]["relationships"]["parent"]["data"][
                                    "id"
                                ] = tid_to_d9_taxonomy_term[term["parent_tid"]]["data"][
                                    "id"
                                ]
                            tid = term["tid"]
                            # Delete tid and parent_tid from data to be POSTed.
                            del term["tid"]
                            del term["parent_tid"]
                            tid_to_d9_taxonomy_term[tid] = post(
                                term,
                                "taxonomy_term",
                                d9_taxonomy_voc,
                                target,
                                targetusername,
                                targetpassword,
                            )
                            terms.remove(term)
                            bar.update(1)

        # Get field configs in D7 DB
        fields = get_fields(connection)

        for bundle in bundles:
            d9bundle = cp["node_bundle_map"].get(bundle, bundle)
            nodes = create_nodes(connection, bundle, d9bundle)
            # For some bundles, we want to omit old nodes.
            if bundle in cp["newer_than"]:
                newer_than = datetime.datetime.strptime(
                    cp["newer_than"]["news"], "%Y-%m-%dT%H:%M:%S"
                )
                nodes = [n for n in nodes if n["changed"] > newer_than]

            if bundle in cp["node_bundle_page_type_map"]:
                for node in nodes:
                    node["data"]["attributes"]["field_page_page_type"] = cp[
                        "node_bundle_page_type_map"
                    ][bundle]

            with click.progressbar(
                nodes,
                label=f"POSTing {bundle} nodes to target",
                item_show_func=lambda node: str(node["nid"])
                if node is not None
                else "",
            ) as bar:
                for node in bar:
                    nid = node["nid"]
                    node = copy.deepcopy(
                        node
                    )  # Progress bar can still use original node
                    del node["nid"]
                    del node["changed"]
                    nid_to_d9_node[nid] = post(
                        node, "node", d9bundle, target, targetusername, targetpassword
                    )
                    nid_to_bundle_name[nid] = d9bundle

        for d7fieldname, d9fieldname in cp.items("field_mappings"):
            with click.progressbar(
                nid_to_d9_node,
                label=f"PATCHing {d7fieldname}",
                item_show_func=lambda nid: str(nid) if nid is not None else "",
            ) as bar:
                for nid in bar:
                    field_config = fields[d7fieldname]
                    bundle = nid_to_bundle_name[nid]
                    # For some entity reference nodes, we want to manually
                    # target particular paragraphs.
                    if (
                        d7fieldname
                        in cp["node_entity_reference_to_paragraph_library_item"]
                    ):
                        field = create_field_paragraph_library_item(
                            cp,
                            connection,
                            bundle,
                            nid,
                            nid_to_d9_node,
                            d7fieldname,
                            d9fieldname,
                            target,
                            targetusername,
                            targetpassword,
                        )
                    else:
                        field = create_field(
                            cp,
                            connection,
                            bundle,
                            nid,
                            nid_to_d9_node,
                            tid_to_d9_taxonomy_term,
                            d7fieldname,
                            d9fieldname,
                            field_config,
                            target,
                            targetusername,
                            targetpassword,
                        )

                    patch(
                        field,
                        "node",
                        bundle,
                        target,
                        targetusername,
                        targetpassword,
                    )


def get_fields(connection):
    fields = {}
    with connection.cursor() as cursor:
        sql = "SELECT * FROM `field_config`"
        cursor.execute(sql)
        for row in cursor:
            fields[row["field_name"]] = {"type": row["type"], "module": row["module"]}
    return fields


def create_taxonomy_terms(connection, d7_machine_name, d9_machine_name):
    terms = []
    with connection.cursor() as cursor:
        sql = (
            "SELECT `taxonomy_term_data`.`tid`, "
            "       `taxonomy_term_data`.`name`, "
            "       `taxonomy_term_hierarchy`.`parent` "
            "FROM `taxonomy_vocabulary` "
            "LEFT JOIN `taxonomy_term_data` "
            "    ON `taxonomy_vocabulary`.`vid` = `taxonomy_term_data`.`vid` "
            "LEFT JOIN `taxonomy_term_hierarchy` "
            "    ON `taxonomy_term_data`.`tid` = `taxonomy_term_hierarchy`.`tid` "
            "WHERE `taxonomy_vocabulary`.`machine_name` = %s"
        )
        cursor.execute(sql, (d7_machine_name,))
        for row in cursor:
            term = {
                "tid": row["tid"],
                "parent_tid": row["parent"],
                "data": {
                    "type": f"taxonomy_vocabulary--{d9_machine_name}",
                    "attributes": {},
                    "relationships": {
                        "parent": {
                            "data": {
                                "type": f"taxonomy_term--{d9_machine_name}",
                                "id": "virtual",
                            },
                        },
                    },
                },
            }
            term["data"]["attributes"]["langcode"] = "en"
            term["data"]["attributes"]["name"] = row["name"].strip()
            terms.append(term)
    return terms


def create_nodes(connection, bundle, d9bundle):
    nodes = []
    with connection.cursor() as cursor:
        sql = "SELECT * FROM `node` WHERE `type`=%s"
        cursor.execute(sql, (bundle,))
        for row in cursor:
            node = {
                "nid": row["nid"],
                "changed": datetime.datetime.fromtimestamp(row["changed"]),
                "data": {"type": f"node--{d9bundle}", "attributes": {}},
            }
            node["data"]["attributes"]["langcode"] = "en"
            node["data"]["attributes"]["title"] = row["title"].strip()
            node["data"]["attributes"]["status"] = row["status"] == 1
            node["data"]["attributes"]["promote"] = row["promote"] == 1
            node["data"]["attributes"]["sticky"] = row["sticky"] == 1
            node["data"]["attributes"]["created"] = datetime.datetime.fromtimestamp(
                row["created"], tz=datetime.timezone.utc
            ).isoformat()
            alias = get_path_alias(connection, row["nid"])
            if alias:
                node["data"]["attributes"]["path"] = {"alias": alias}
            nodes.append(node)
    return nodes


def get_path_alias(connection, nid):
    with connection.cursor() as cursor:
        sql = "SELECT `alias` FROM `url_alias` WHERE `source`=%s"
        cursor.execute(sql, (f"node/{nid}",))
        row = cursor.fetchone()
        if row:
            alias = row["alias"]
            if not alias.startswith("/"):
                alias = "/" + alias
            return alias
        return None


def get_path_from_fid(connection, fid):
    with connection.cursor() as cursor:
        sql = "SELECT `filename`, `uri` FROM `file_managed` WHERE `fid`=%s"
        cursor.execute(sql, (fid,))
        row = cursor.fetchone()
        if row is None:
            raise ValueError(f"Unable to find file path for fid {fid}")
        if not row["uri"].startswith("public://"):
            raise ValueError(f"Trying to send private file for fid {fid}")
        return (
            row["uri"].replace(
                "public://", "/var/www/drupal/drupal-root/sites/default/files/"
            ),
            row["filename"],
        )


def create_field(
    config,
    connection,
    bundle,
    nid,
    nid_to_d9_node,
    tid_to_d9_taxonomy_term,
    d7fieldname,
    d9fieldname,
    field_config,
    target,
    targetusername,
    targetpassword,
):
    field = {
        "data": {
            "type": f"node--{bundle}",
            "id": nid_to_d9_node[nid]["data"]["id"],
            "attributes": {},
            "relationships": {},
        }
    }
    with connection.cursor() as cursor:
        sql = (
            f"SELECT * FROM `field_data_{d7fieldname}` "
            "WHERE `entity_type` = 'node' AND `entity_id` = %s "
            "ORDER BY `delta`"
        )
        cursor.execute(sql, (nid,))
        attributes = []
        relationships = []
        for row in cursor:
            if field_config["type"] == "link_field":
                field_data = {}
                field_data["uri"] = clean_uri(row[f"{d7fieldname}_url"], nid_to_d9_node)
                if field_data["uri"] == "":
                    field_data["uri"] = "route:<nolink>"
                field_data["title"] = row[f"{d7fieldname}_title"]
                attributes.append(field_data)
            elif field_config["type"] == "text" or field_config["type"] == "text_long":
                if (
                    d7fieldname in config["concatinate_text_field"]
                    and len(attributes) > 0
                ):
                    if row[f"{d7fieldname}_format"] is None:
                        attributes[0] = (
                            attributes[0]
                            + config["concatinate_text_field"][d7fieldname]
                            + row[f"{d7fieldname}_value"]
                        )
                    else:
                        attributes[0]["value"] = (
                            attributes[0]["value"]
                            + config["concatinate_text_field"][d7fieldname]
                            + row[f"{d7fieldname}_value"]
                        )
                else:
                    # If the d7 text field has a null format,
                    # the d9 text field is plain.
                    if (
                        row[f"{d7fieldname}_format"] is None
                        or d7fieldname in config["formatted_to_plain"]
                    ):
                        attributes.append(row[f"{d7fieldname}_value"])
                    else:
                        field_data = {}
                        field_data["value"] = clean_text(
                            row[f"{d7fieldname}_value"], nid_to_d9_node
                        )
                        field_data["format"] = config["text_format_mappings"][
                            str(row[f"{d7fieldname}_format"])
                        ]
                        attributes.append(field_data)
            elif field_config["type"] == "text_with_summary":
                if str(row[f"{d7fieldname}_format"]) != "0":
                    field_data = {}
                    field_data["value"] = clean_text(
                        row[f"{d7fieldname}_value"], nid_to_d9_node
                    )
                    field_data["summary"] = clean_text(
                        row[f"{d7fieldname}_summary"], nid_to_d9_node
                    )
                    field_data["format"] = config["text_format_mappings"][
                        str(row[f"{d7fieldname}_format"])
                    ]
                    attributes.append(field_data)
            elif field_config["type"] == "list_text":
                if d7fieldname in config["list_text_to_boolean"]:
                    attributes.append(
                        str(row[f"{d7fieldname}_value"]).strip()
                        == config["list_text_to_boolean"][d7fieldname]
                    )
                else:
                    attributes.append(row[f"{d7fieldname}_value"])
            elif field_config["type"] == "datetime":
                attributes.append(
                    row[f"{d7fieldname}_value"].strftime(
                        config["date_format_mappings"][d7fieldname]
                    )
                )
            elif field_config["type"] == "list_boolean":
                attributes.append(bool(row[f"{d7fieldname}_value"]))
            elif field_config["type"] == "taxonomy_term_reference":
                relationship_data = {}
                d9_taxonomy_term = tid_to_d9_taxonomy_term[row[f"{d7fieldname}_tid"]]
                relationship_data["type"] = d9_taxonomy_term["data"]["type"]
                relationship_data["id"] = d9_taxonomy_term["data"]["id"]
                relationships.append(relationship_data)
            elif field_config["type"] == "entityreference":
                relationship_data = {}
                d9_node = nid_to_d9_node[row[f"{d7fieldname}_target_id"]]
                relationship_data["type"] = d9_node["data"]["type"]
                relationship_data["id"] = d9_node["data"]["id"]
                relationships.append(relationship_data)
            elif field_config["type"] == "image":
                path, filename = get_path_from_fid(
                    connection, row[f"{d7fieldname}_fid"]
                )
                file_resp = post_image_file(
                    path, filename, target, targetusername, targetpassword
                )
                media_image_data = {
                    "data": {
                        "attributes": {"name": filename},
                        "type": "media--image",
                        "relationships": {
                            "field_media_image": {
                                "data": {
                                    "type": "file--file",
                                    "id": file_resp["data"]["id"],
                                    "meta": {
                                        "alt": row[f"{d7fieldname}_alt"],
                                    },
                                }
                            }
                        },
                    }
                }
                media_resp = post(
                    media_image_data,
                    "media",
                    "image",
                    target,
                    targetusername,
                    targetpassword,
                )
                relationship_data = {}
                relationship_data["type"] = "media--image"
                relationship_data["id"] = media_resp["data"]["id"]
                relationships.append(relationship_data)
            else:
                raise ValueError(
                    f"Unexpected field type encountered: {field_config['type']}"
                )
        if attributes:
            field["data"]["attributes"][d9fieldname] = attributes
        if relationships:
            field["data"]["relationships"][d9fieldname] = {"data": relationships}
    return field


def create_field_paragraph_library_item(
    config,
    connection,
    bundle,
    nid,
    nid_to_d9_node,
    d7fieldname,
    d9fieldname,
    target,
    targetusername,
    targetpassword,
):
    field = {
        "data": {
            "type": f"node--{bundle}",
            "id": nid_to_d9_node[nid]["data"]["id"],
            "relationships": {},
            "attributes": {},
        }
    }
    with connection.cursor() as cursor:
        sql = (
            f"SELECT * FROM `field_data_{d7fieldname}` "
            "WHERE `entity_type` = 'node' AND `entity_id` = %s "
            "ORDER BY `delta`"
        )
        cursor.execute(sql, (nid,))
        relationships = []
        for row in cursor:
            from_library = {
                "data": {
                    "attributes": {
                        "parent_field_name": d9fieldname,
                        "parent_id": nid_to_d9_node[nid]["data"]["attributes"][
                            "drupal_internal__nid"
                        ],
                        "parent_type": "node",
                    },
                    "relationships": {
                        "field_reusable_paragraph": {
                            "data": {
                                "id": config["node_to_paragraph_library_item_map"][str(row[f"{d7fieldname}_target_id"])],
                                "type": "paragraphs_library_item--paragraphs_library_item",
                            }
                        }
                    },
                    "type": "paragraph--from_library",
                }
            }

            new_from_library = post(
                from_library,
                "paragraph",
                "from_library",
                target,
                targetusername,
                targetpassword,
            )
            relationship_data = {}
            relationship_data["type"] = "paragraph--from_library"
            relationship_data["id"] = new_from_library["data"]["id"]
            relationship_data["meta"] = {}
            relationship_data["meta"]["target_revision_id"] = new_from_library["data"][
                "attributes"
            ]["drupal_internal__revision_id"]
            relationships.append(relationship_data)
        if relationships:
            field["data"]["relationships"][d9fieldname] = {"data": relationships}
    return field


def clean_uri(uri, nid_to_d9_node):
    # Normalize all internal links
    if uri.startswith("node/"):
        uri = "https://library.carleton.ca/" + uri
    if uri.startswith("/node/"):
        uri = "https://library.carleton.ca" + uri
    if uri.startswith("https://library.carleton.ca/node/"):
        nid = uri[len("https://library.carleton.ca/node/") :]
        if nid[-1] == "/":
            nid = nid[:-1]
        try:
            d9node = nid_to_d9_node[int(nid)]
        except KeyError:
            click.echo(f"WARNING: Unable to find D9 Node for {nid}.")
            d9node = {"data": {"attributes": {"drupal_internal__nid": "fourohfour"}}}
        uri = "internal:/node/" + str(
            d9node["data"]["attributes"]["drupal_internal__nid"]
        )
    # Ensure all links to proxy are https
    if uri.startswith("proxy.library.carleton.ca"):
        uri = "https://" + uri
    # Use qurl param to proxy instead of url
    if uri.startswith("https://proxy.library.carleton.ca/login?url="):
        uri = uri.replace("url", "qurl", 1)
    # Fix help links
    if uri.startswith("help/"):
        uri = "internal:/" + uri
    if uri.startswith("/help/"):
        uri = "internal:/" + uri[1:]
    return uri


def clean_text(value, nid_to_d9_node):
    # Pass any links through the clean_uri function
    re.sub(
        r'href="(?P<href>[^"]+)"',
        lambda m: 'href="' + clean_uri(m.group("href"), nid_to_d9_node) + '"',
        value,
    )
    return value


def post(obj, entity, bundle, target, targetusername, targetpassword):
    url = target + "/" + entity + "/" + bundle
    headers = {
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }
    resp = requests.post(
        url,
        headers=headers,
        data=json.dumps(obj),
        auth=(targetusername, targetpassword),
    )
    try:
        resp.raise_for_status()
    except requests.RequestException:
        click.echo(pprint.pformat(obj))
        try:
            click.echo(pprint.pformat(resp.json()))
        except json.decoder.JSONDecodeError:
            click.echo(resp.text)
        raise
    return resp.json()


def post_image_file(path, filename, target, targetusername, targetpassword):
    url = target + "/media/image/field_media_image"
    headers = {
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/octet-stream",
        "Content-Disposition": f'file; filename="{filename}"',
    }
    with open(path, "rb") as upload_file:
        resp = requests.post(
            url,
            headers=headers,
            data=upload_file,
            auth=(targetusername, targetpassword),
        )
    try:
        resp.raise_for_status()
    except requests.RequestException:
        click.echo(path)
        try:
            click.echo(pprint.pformat(resp.json()))
        except json.decoder.JSONDecodeError:
            click.echo(resp.text)
        raise
    return resp.json()


def patch(obj, entity, bundle, target, targetusername, targetpassword):
    # If the attributes of the field are blank, don't bother PATCHing
    if not obj["data"]["attributes"] and not obj["data"]["relationships"]:
        return
    url = target + "/" + entity + "/" + bundle + "/" + obj["data"]["id"]
    headers = {
        "Accept": "application/vnd.api+json",
        "Content-Type": "application/vnd.api+json",
    }
    resp = requests.patch(
        url,
        headers=headers,
        data=json.dumps(obj),
        auth=(targetusername, targetpassword),
    )
    try:
        resp.raise_for_status()
    except requests.RequestException:
        click.echo(pprint.pformat(obj))
        try:
            click.echo(pprint.pformat(resp.json()))
        except json.decoder.JSONDecodeError:
            click.echo(resp.text)
        raise
