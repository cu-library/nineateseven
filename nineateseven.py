# -*- coding: utf-8 -*-
"""A CLI tool for migrating our Drupal 7 site to Drupal 9"""

import api
import click
import configparser
import datetime
import pymysql
import re
import sys

CONTEXT_SETTINGS = {"auto_envvar_prefix": "NINEATESEVEN"}


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option("--db", default="drupal", show_default=True)
@click.option("--dbcharset", default="utf8mb4", show_default=True)
@click.option("--dbusername", "-dbu", default="drupal", show_default=True)
@click.option("--dbpassword", "-dbp", required=True)
@click.option("--target", "-t", required=True)
@click.option("--targetusername", "-tu", required=True)
@click.option("--targetpassword", "-tp", required=True)
@click.option("--mapping", "-m", "mappingfile", type=click.File(), required=True)
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
    mappingfile,
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

    # Pesky security warning particular to Carleton's environment.
    if disablesubjectaltnamewarning:
        import warnings
        import urllib3

        warnings.simplefilter("ignore", urllib3.exceptions.SubjectAltNameWarning)

    for bundle in bundles:
        # Exit early for unknown bundles.
        if bundle not in [
            "news",
            "database",
            "geospatial_data",
            "policy",
            "transcript",
            "guide",
            "service",
            "help_guide",
            "course_guide",
            "collection_page",
            "find_guide",
            "page",
            "survey_data",
            "subject_detailed_guide",
            "subject_quick_guide",
        ]:
            sys.exit(f"Unknown bundle name {bundle}")

    # Read in the mapping file.
    mapping = configparser.ConfigParser(interpolation=None)
    mapping.read_file(mappingfile)

    # Create the Drupal API object.
    drupal = api.DrupalAPI(target, targetusername, targetpassword)
    if not drupal.test():
        sys.exit("Unable to connect to Drupal API.")

    click.echo("Using mapping file to load existing D9 objects...", nl=False)
    nid_to_existing_obj = load_objs_from_mapping(mapping, drupal)
    click.echo("Done!")

    nid_to_new_obj = {}

    # Connect to the local MySQL database.
    connection = pymysql.connect(
        host="localhost",
        database=db,
        charset=dbcharset,
        user=dbusername,
        password=dbpassword,
        cursorclass=pymysql.cursors.DictCursor,
    )

    # Context manager around the connection, so it will be automatically closed.
    with connection:

        # Create a mapping between quick and detailed subject guides.
        subject_guide_quick_to_detailed = find_subject_guide_pairs(connection)
        subject_guide_detailed_nid_to_obj = {}

        for bundle in bundles:
            click.echo(f"{bundle}...", nl=False)
            if bundle == "news":
                nid_to_new_obj.update(migrate_news_nodes(connection, drupal, mapping))
            elif bundle == "database":
                nid_to_new_obj.update(
                    migrate_database_nodes(connection, drupal, mapping)
                )
            elif bundle == "geospatial_data":
                nid_to_new_obj.update(
                    migrate_geospatial_data_nodes(connection, drupal, mapping)
                )
            elif bundle == "policy":
                nid_to_new_obj.update(migrate_policy_nodes(connection, drupal, mapping))
            elif bundle == "transcript":
                nid_to_new_obj.update(
                    migrate_transcript_nodes(connection, drupal, mapping)
                )
            elif bundle == "guide":
                nid_to_new_obj.update(migrate_guide_nodes(connection, drupal, mapping))
            elif bundle == "service":
                nid_to_new_obj.update(
                    migrate_service_nodes(connection, drupal, mapping)
                )
            elif bundle == "help_guide":
                nid_to_new_obj.update(
                    migrate_help_guide_nodes(connection, drupal, mapping)
                )
            elif bundle == "course_guide":
                nid_to_new_obj.update(
                    migrate_course_guide_nodes(connection, drupal, mapping)
                )
            elif bundle == "collection_page":
                nid_to_new_obj.update(
                    migrate_collection_page_nodes(connection, drupal, mapping)
                )
            elif bundle == "find_guide":
                nid_to_new_obj.update(
                    migrate_find_guide_nodes(connection, drupal, mapping)
                )
            elif bundle == "page":
                nid_to_new_obj.update(migrate_page_nodes(connection, drupal, mapping))
            elif bundle == "survey_data":
                nid_to_new_obj.update(
                    migrate_survey_data_nodes(connection, drupal, mapping)
                )
            elif bundle == "subject_detailed_guide":
                nid_to_new_obj.update(
                    migrate_subject_detailed_guide_nodes(
                        connection, drupal, mapping, subject_guide_quick_to_detailed
                    )
                )
            elif bundle == "subject_quick_guide":
                nid_to_new_obj.update(
                    migrate_subject_quick_guide_nodes(connection, drupal, mapping)
                )
                for quick_nid, detailed_nid in subject_guide_quick_to_detailed.items():
                    subject_guide_detailed_nid_to_obj[detailed_nid] = nid_to_new_obj[
                        quick_nid
                    ]
            click.echo("Done!")

        for nid, obj in nid_to_new_obj.items():
            node_type = load_type(connection, nid)
            click.echo(
                (
                    f"{nid} to {obj['data']['attributes']['drupal_internal__nid']}, "
                    f"{node_type} to {obj['data']['type']}..."
                ),
                nl=False,
            )
            if node_type == "news":
                migrate_news_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                )
            elif node_type == "database":
                migrate_database_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
            elif node_type == "geospatial_data":
                migrate_geospatial_data_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
            elif node_type == "policy":
                migrate_policy_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                )
            elif node_type == "transcript":
                migrate_transcript_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                )

            elif node_type == "guide":
                migrate_guide_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
            # The only books we migrate from node to node are now services.
            elif node_type == "service" or node_type == "book":
                migrate_service_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
            elif node_type == "help_guide":
                migrate_help_guide_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
            elif node_type == "course_guide":
                migrate_course_guide_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
            elif node_type == "collection_page":
                migrate_collection_page_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
            elif node_type == "find_guide":
                migrate_find_guide_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                )
            elif node_type == "page":
                migrate_page_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                )
            elif node_type == "survey_data":
                migrate_survey_data_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                )
            elif node_type == "subject_detailed_guide":
                migrate_subject_detailed_guide_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
            elif node_type == "subject_quick_guide":
                migrate_subject_quick_guide_fields(
                    connection,
                    drupal,
                    nid,
                    obj,
                    {**nid_to_existing_obj, **nid_to_new_obj},
                    mapping,
                )
                if nid in subject_guide_quick_to_detailed:
                    migrate_subject_detailed_guide_fields(
                        connection,
                        drupal,
                        subject_guide_quick_to_detailed[nid],
                        obj,
                        {
                            **nid_to_existing_obj,
                            **nid_to_new_obj,
                            **subject_guide_detailed_nid_to_obj,
                        },
                        mapping,
                    )
            click.echo("Done!")

        click.echo("")
        click.echo("")
        click.echo("New d7 nid to uuid mappings")
        for nid, obj in nid_to_new_obj.items():
            click.echo(f"{nid} = {obj['data']['id']}")
        click.echo("")

        click.echo("New d7 nid to type mappings")
        for nid, obj in nid_to_new_obj.items():
            click.echo(f"{nid} = {obj['data']['type']}")
        click.echo("")


def find_subject_guide_pairs(connection):
    pairs = {}
    with connection.cursor() as cursor:
        sql = (
            "SELECT entity_id, field_link_to_detailed_guide_target_id "
            "FROM field_data_field_link_to_detailed_guide"
        )
        cursor.execute(sql)
        for row in cursor:
            pairs[row["entity_id"]] = row["field_link_to_detailed_guide_target_id"]
    return pairs


# News Nodes


def migrate_news_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(connection, "news", "node--news", mapping)

    # We don't want to migrate old news nodes.
    cutoff = datetime.datetime(2020, 1, 1)
    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if node_newer_than_cutoff(connection, nid, cutoff)
        and str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_news_fields(connection, drupal, nid, obj, nid_to_obj):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Body
    patch_ready_obj["data"]["attributes"][
        "body"
    ] = text_with_summary_to_text_with_summary(connection, "body", nid, nid_to_obj)

    # field_news_category
    patch_ready_obj["data"]["attributes"]["field_news_category"] = news_category(
        connection, nid
    )

    drupal.patch(patch_ready_obj)


def news_category(connection, nid):
    rows = load_field_data(connection, "field_news_category", nid)
    news_category = []
    for row in rows:
        if row["field_news_category_tid"] == 1213:
            news_category.append("Database Downtime")
        elif row["field_news_category_tid"] == 1219:
            news_category.append("New Databases")
    return news_category


# Database Nodes


def migrate_database_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "database", "node--database", mapping
    )

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_database_fields(connection, drupal, nid, obj, nid_to_obj, mapping):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Advisory
    patch_ready_obj["data"]["attributes"][
        "field_database_advisory"
    ] = formatted_text_to_formatted_text(
        connection, "field_database_advisory", nid, nid_to_obj
    )

    # Alternate Spellings
    patch_ready_obj["data"]["attributes"][
        "field_database_alt_spellings"
    ] = text_to_plain_text(connection, "field_alternate_spellings", nid)

    # Alternate Titles
    patch_ready_obj["data"]["attributes"][
        "field_database_alternate_titles"
    ] = text_to_plain_text(connection, "field_database_alternate_titles", nid)

    # Author
    patch_ready_obj["data"]["attributes"]["field_database_author"] = text_to_plain_text(
        connection, "field_database_author", nid
    )

    # Authorized Users
    patch_ready_obj["data"]["attributes"][
        "field_database_authorized_users"
    ] = text_list_to_text_list(connection, "field_database_authorized_users", nid)

    # Brief Description
    patch_ready_obj["data"]["attributes"][
        "field_database_brief_description"
    ] = formatted_text_to_formatted_text(
        connection, "field_database_brief_description", nid, nid_to_obj
    )

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Date Coverage
    patch_ready_obj["data"]["attributes"][
        "field_database_date_coverage"
    ] = text_to_plain_text(connection, "field_database_date_coverage", nid)

    # Description
    patch_ready_obj["data"]["attributes"][
        "field_database_description"
    ] = formatted_text_to_formatted_text(
        connection, "field_database_description", nid, nid_to_obj
    )

    # Fulltext
    patch_ready_obj["data"]["attributes"][
        "field_database_fulltext"
    ] = database_fulltext(connection, nid)

    # Important Details
    patch_ready_obj["data"]["attributes"][
        "field_database_important_details"
    ] = formatted_text_to_formatted_text(
        connection, "field_database_notes", nid, nid_to_obj
    )

    # Link
    patch_ready_obj["data"]["attributes"]["field_database_link"] = link_to_link(
        connection, "field_database_link", nid, nid_to_obj
    )

    # Publisher
    patch_ready_obj["data"]["attributes"][
        "field_database_publisher"
    ] = text_to_plain_text(connection, "field_database_publisher", nid)

    # Subject
    patch_ready_obj["data"]["relationships"]["field_database_subject"] = {
        "data": taxonomy_term_reference_to_taxonomy_term_reference(
            connection, "field_subject", nid, mapping
        )
    }

    # Trial Feedback
    patch_ready_obj["data"]["attributes"][
        "field_database_trial_feedback"
    ] = database_trial_feedback(connection, nid)

    # Type
    patch_ready_obj["data"]["relationships"]["field_database_type"] = {
        "data": taxonomy_term_reference_to_taxonomy_term_reference(
            connection, "field_database_type", nid, mapping
        )
    }

    drupal.patch(patch_ready_obj)


def database_fulltext(connection, nid):
    rows = load_field_data(connection, "field_database_fulltext", nid)
    for row in rows:
        return row["field_database_fulltext_value"] == "Fulltext"
    return False


def database_trial_feedback(connection, nid):
    rows = load_field_data(connection, "field_trial_feedback", nid)
    for row in rows:
        return row["field_trial_feedback_value"] == "yes"
    return False


# Geospatial Data Nodes


def migrate_geospatial_data_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "geospatial_data", "node--geospatial_data", mapping
    )

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_geospatial_data_fields(connection, drupal, nid, obj, nid_to_obj, mapping):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Authorized Users
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_authorized_user"
    ] = text_list_to_text_list(connection, "field_gis_authorized_users", nid)

    # Available Online
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_available_onlin"
    ] = formatted_text_to_formatted_text(
        connection, "field_available_online", nid, nid_to_obj
    )

    # Available Themes
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_available_theme"
    ] = formatted_text_to_formatted_text(
        connection, "field_gis_available_themes", nid, nid_to_obj
    )

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Data Format
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_data_format"
    ] = link_to_link(connection, "field_gis_data_format", nid, nid_to_obj)

    # Data Producer
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_data_producer"
    ] = gis_author(connection, nid)

    # Description
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_description"
    ] = formatted_text_to_formatted_text(
        connection, "field_gis_description", nid, nid_to_obj
    )

    # Disclaimer
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_disclaimer"
    ] = formatted_text_to_formatted_text(
        connection, "field_gis_disclaimer", nid, nid_to_obj
    )

    # Geographic Area
    patch_ready_obj["data"]["relationships"]["field_geospatial_geographic_area"] = {
        "data": taxonomy_term_reference_to_taxonomy_term_reference(
            connection, "field_geo_data_geographic_area", nid, mapping
        )
    }

    # GIS Topic
    patch_ready_obj["data"]["relationships"]["field_geospatial_gis_topic"] = {
        "data": taxonomy_term_reference_to_taxonomy_term_reference(
            connection, "field_geo_data_gis_topic", nid, mapping
        )
    }

    # Interactive Index
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_interactive_ind"
    ] = formatted_text_to_formatted_text(
        connection, "field_gis_interactive_index", nid, nid_to_obj
    )

    # Location
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_location"
    ] = formatted_text_to_formatted_text(
        connection, "field_gis_location", nid, nid_to_obj
    )

    # Other Metadata
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_other_metadata"
    ] = formatted_text_to_formatted_text(
        connection, "field_gis_other_metadata", nid, nid_to_obj
    )

    # Projection
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_projection"
    ] = formatted_text_to_formatted_text(
        connection, "field_gis_projection", nid, nid_to_obj
    )

    # Publication Date
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_publication_dat"
    ] = text_to_plain_text(connection, "field_gis_publication_date", nid)

    # Related Databases
    patch_ready_obj["data"]["relationships"]["field_related_databases"] = {
        "data": entity_reference_to_entity_reference(
            connection, "field_related_databases", nid, nid_to_obj
        )
    }

    # Related Geospatial Data
    patch_ready_obj["data"]["relationships"]["field_related_geospatial_data"] = {
        "data": entity_reference_to_entity_reference(
            connection, "field_related_gis_data", nid, nid_to_obj
        )
    }

    # Related Help  field_related_help  Entity reference field_related_help
    # TODO HERE

    # Resolution
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_resolution"
    ] = text_to_plain_text(connection, "field_gis_resolution", nid)

    # Sample Image
    patch_ready_obj["data"]["relationships"]["field_geospatial_sample_image"] = {
        "data": image(connection, "field_gis_sample_image", drupal, nid)
    }

    # Scale
    patch_ready_obj["data"]["attributes"][
        "field_geospatial_scale"
    ] = text_to_plain_text(connection, "field_gis_scale", nid)

    drupal.patch(patch_ready_obj)


def gis_author(connection, nid):
    rows = load_field_data(connection, "field_gis_author", nid)
    authors = []
    for row in rows:
        authors.append(row["field_gis_author_value"])
    return {
        "value": ", ".join(authors),
        "format": "plain_text",
    }


# Policy Nodes


def migrate_policy_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(connection, "policy", "node--page", mapping)

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_policy_fields(connection, drupal, nid, obj, nid_to_obj):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Body
    patch_ready_obj["data"]["attributes"][
        "body"
    ] = text_with_summary_to_text_with_summary(connection, "body", nid, nid_to_obj)

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Content type
    patch_ready_obj["data"]["attributes"]["field_content_type"] = "Policy"

    drupal.patch(patch_ready_obj)


# Transcript Nodes


def migrate_transcript_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "transcript", "node--transcript", mapping
    )

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_transcript_fields(connection, drupal, nid, obj, nid_to_obj):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Body
    patch_ready_obj["data"]["attributes"][
        "body"
    ] = text_with_summary_to_text_with_summary(connection, "body", nid, nid_to_obj)

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    drupal.patch(patch_ready_obj)


# Guide Nodes


def migrate_guide_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(connection, "guide", "node--guide", mapping)

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_guide_fields(connection, drupal, nid, obj, nid_to_obj, mapping):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Guide sections
    patch_ready_obj["data"]["relationships"]["field_guide_section"] = {"data": []}

    # - Body
    patch_ready_obj["data"]["relationships"]["field_guide_section"]["data"].extend(
        text_with_summary_to_text_area_paragraph(
            connection, "body", "field_guide_section", drupal, nid, nid_to_obj
        )
    )

    # - Field Guide Detailed Sections
    patch_ready_obj["data"]["relationships"]["field_guide_section"]["data"].extend(
        detailed_guide_section_to_accordion_paragraph(
            connection, "field_guide_section", drupal, nid, nid_to_obj
        )
    )

    # Guide Type
    patch_ready_obj["data"]["attributes"]["field_guide_type"] = "Help"

    drupal.patch(patch_ready_obj)


# Service Nodes


def migrate_service_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "service", "node--service", mapping
    )

    books = load_objs_from_database(connection, "book", "node--service", mapping)
    subpages = {}

    for nid in nid_to_obj:
        with connection.cursor() as cursor:
            sql = "SELECT `nid` FROM `book` WHERE `bid`=%s"
            cursor.execute(sql, (nid,))
            for row in cursor:
                if row["nid"] in books:
                    subpages[row["nid"]] = books[row["nid"]]

    nid_to_obj.update(subpages)

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_service_fields(connection, drupal, nid, obj, nid_to_obj, mapping):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Action Link
    patch_ready_obj["data"]["attributes"]["field_service_action_link"] = link_to_link(
        connection, "field_action_link", nid, nid_to_obj
    )

    # Body
    node_type = load_type(connection, nid)
    if node_type == "book":
        patch_ready_obj["data"]["attributes"][
            "body"
        ] = text_with_summary_to_text_with_summary(connection, "body", nid, nid_to_obj)
    else:
        patch_ready_obj["data"]["attributes"][
            "body"
        ] = formatted_text_to_formatted_text(
            connection, "field_full_description", nid, nid_to_obj
        )

    # Contact / Service point
    patch_ready_obj["data"]["relationships"]["field_contact_service_point"] = {
        "data": contact_service_point(connection, drupal, nid, nid_to_obj)
    }

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Eligibility
    patch_ready_obj["data"]["attributes"]["field_eligibility"] = text_list_to_text_list(
        connection, "field_service_eligibility", nid
    )

    # Service Category
    patch_ready_obj["data"]["relationships"]["field_service_category"] = {
        "data": taxonomy_term_reference_to_taxonomy_term_reference(
            connection, "field_service_category", nid, mapping
        )
    }

    # Short description
    patch_ready_obj["data"]["attributes"][
        "field_short_description"
    ] = formatted_text_to_formatted_text(
        connection, "field_brief_description", nid, nid_to_obj
    )

    drupal.patch(patch_ready_obj)


# Help Guides Nodes


def migrate_help_guide_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "help_guide", "node--guide", mapping
    )

    for nid in nid_to_obj:
        try:
            nid_to_obj[nid]["data"]["attributes"]["path"]["alias"] = (
                nid_to_obj[nid]["data"]["attributes"]["path"]["alias"].replace("help-guides", "guides/help")
            )
        except KeyError:
            pass

    do_not_migrate = [
        "113",
        "114",
        "12647",
        "12648",
        "12649",
        "12650",
        "12651",
        "12652",
        "12652",
        "12653",
        "12654",
        "12655",
        "12821",
        "12822",
        "12823",
        "12824",
        "12825",
        "12826",
        "12844",
        "12845",
        "12846",
        "12900",
        "12901",
        "12908",
        "12909",
        "12910",
        "12911",
        "12912",
        "12913",
        "13073",
        "13074",
        "13076",
        "13169",
        "15476",
        "15477",
        "15480",
        "15481",
        "15482",
        "15483",
        "15484",
        "15485",
        "15486",
        "15487",
        "15488",
        "15506",
        "15509",
        "15510",
        "15511",
        "15513",
        "15514",
        "15515",
        "15516",
        "15517",
        "15518",
        "15519",
        "15669",
        "15670",
        "15672",
        "15674",
        "15675",
        "15676",
        "15679",
        "15680",
        "15681",
        "15682",
        "15683",
        "15685",
        "15686",
        "15687",
        "15688",
        "15690",
        "15691",
        "15694",
        "15695",
        "15696",
        "15697",
        "15698",
    ]

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
        and str(nid) not in do_not_migrate
    }


def migrate_help_guide_fields(connection, drupal, nid, obj, nid_to_obj, mapping):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Contact / Service point
    patch_ready_obj["data"]["relationships"]["field_contact_service_point"] = {
        "data": contact_service_point(connection, drupal, nid, nid_to_obj)
    }

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Guide sections
    patch_ready_obj["data"]["relationships"]["field_guide_section"] = {"data": []}

    # - Body
    patch_ready_obj["data"]["relationships"]["field_guide_section"]["data"].extend(
        text_with_summary_to_text_area_paragraph(
            connection, "body", "field_guide_section", drupal, nid, nid_to_obj
        )
    )

    # - Subpages
    patch_ready_obj["data"]["relationships"]["field_guide_section"]["data"].extend(
        subpage_to_accordion_paragraph(
            connection, "field_guide_section", drupal, nid, nid_to_obj
        )
    )

    # Guide Type
    patch_ready_obj["data"]["attributes"]["field_guide_type"] = "Help"

    # TODO Add redirects from children to new anchored accordions.
    # /help/[parent-title]/[child-title] becomes
    # /guides/help/[parent-title]#[child-title]

    drupal.patch(patch_ready_obj)


# Course Guides Nodes


def migrate_course_guide_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "course_guide", "node--guide", mapping
    )

    for nid in nid_to_obj:
        try:
            nid_to_obj[nid]["data"]["attributes"]["path"]["alias"] = (
                nid_to_obj[nid]["data"]["attributes"]["path"]["alias"].replace("research/course-guides", "guides/course")
            )
        except KeyError:
            pass

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_course_guide_fields(connection, drupal, nid, obj, nid_to_obj, mapping):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Guide sections
    patch_ready_obj["data"]["relationships"]["field_guide_section"] = {"data": []}

    # - Body
    patch_ready_obj["data"]["relationships"]["field_guide_section"]["data"].extend(
        text_with_summary_to_text_area_paragraph(
            connection, "body", "field_guide_section", drupal, nid, nid_to_obj
        )
    )

    # - Subpages
    patch_ready_obj["data"]["relationships"]["field_guide_section"]["data"].extend(
        subpage_to_accordion_paragraph(
            connection, "field_guide_section", drupal, nid, nid_to_obj
        )
    )

    # Guide Type
    patch_ready_obj["data"]["attributes"]["field_guide_type"] = "Course"

    # TODO Add redirects from children to new anchored accordions.
    # /research/course-guides/[parent-title]/[child-title] becomes
    # /guides/course/[parent-title]#[child-title]

    drupal.patch(patch_ready_obj)


# Collection Page Nodes


def migrate_collection_page_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "collection_page", "node--collection_page", mapping
    )

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_collection_page_fields(connection, drupal, nid, obj, nid_to_obj, mapping):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Body
    patch_ready_obj["data"]["attributes"][
        "body"
    ] = text_with_summary_to_text_with_summary(connection, "body", nid, nid_to_obj)

    # Collection
    patch_ready_obj["data"]["relationships"]["field_collection_page_collection"] = {
        "data": taxonomy_term_reference_to_taxonomy_term_reference(
            connection, "field_collection", nid, mapping
        )
    }

    # Contact / Service point
    patch_ready_obj["data"]["relationships"]["field_contact_service_point"] = {
        "data": contact_service_point(connection, drupal, nid, nid_to_obj)
    }

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    drupal.patch(patch_ready_obj)


# Find Guide Nodes


def migrate_find_guide_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "find_guide", "node--find", mapping
    )

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_find_guide_fields(connection, drupal, nid, obj, nid_to_obj):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Body
    patch_ready_obj["data"]["attributes"][
        "body"
    ] = text_with_summary_to_text_with_summary(connection, "body", nid, nid_to_obj)

    # Contact / Service point
    patch_ready_obj["data"]["relationships"]["field_contact_service_point"] = {
        "data": contact_service_point(connection, drupal, nid, nid_to_obj)
    }

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    drupal.patch(patch_ready_obj)


# Page Nodes


def migrate_page_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(connection, "page", "node--page", mapping)

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_page_fields(connection, drupal, nid, obj, nid_to_obj):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Body
    patch_ready_obj["data"]["attributes"][
        "body"
    ] = text_with_summary_to_text_with_summary(connection, "body", nid, nid_to_obj)

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    drupal.patch(patch_ready_obj)


# Survey Data Nodes


def migrate_survey_data_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "survey_data", "node--page", mapping
    )

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_survey_data_fields(connection, drupal, nid, obj, nid_to_obj):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Body
    patch_ready_obj["data"]["attributes"][
        "body"
    ] = text_with_summary_to_text_with_summary(connection, "body", nid, nid_to_obj)

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Content type
    patch_ready_obj["data"]["attributes"]["field_content_type"] = "SurveyData"

    drupal.patch(patch_ready_obj)


# Detailed Subject Guide Nodes


def migrate_subject_detailed_guide_nodes(
    connection, drupal, mapping, subject_guide_quick_to_detailed
):
    nid_to_obj = load_objs_from_database(
        connection, "subject_detailed_guide", "node--guide", mapping
    )

    for nid in nid_to_obj:
        try:
            title = nid_to_obj[nid]["data"]["attributes"]["path"]["alias"].split("/")[
                3
            ][:-15]
            nid_to_obj[nid]["data"]["attributes"]["path"]["alias"] = (
                "/guides/subject/" + title
            )
        except KeyError:
            pass

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
        and nid not in subject_guide_quick_to_detailed.values()
    }


def migrate_subject_detailed_guide_fields(
    connection, drupal, nid, obj, nid_to_obj, mapping
):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Guide sections
    patch_ready_obj["data"]["relationships"]["field_guide_section"] = {"data": []}

    # - Body
    patch_ready_obj["data"]["relationships"]["field_guide_section"]["data"].extend(
        text_with_summary_to_text_area_paragraph(
            connection, "body", "field_guide_section", drupal, nid, nid_to_obj
        )
    )

    # - Field Guide Detailed Sections
    patch_ready_obj["data"]["relationships"]["field_guide_section"]["data"].extend(
        detailed_guide_section_to_accordion_paragraph(
            connection, "field_guide_section", drupal, nid, nid_to_obj
        )
    )

    # Guide Type
    patch_ready_obj["data"]["attributes"]["field_guide_type"] = "Subject"

    # Subject Category
    patch_ready_obj["data"]["relationships"]["field_guide_subject_category"] = {
        "data": taxonomy_term_reference_to_taxonomy_term_reference(
            connection, "field_subject_category", nid, mapping
        )
    }

    drupal.patch(patch_ready_obj)


# Quick Subject Guide Nodes


def migrate_subject_quick_guide_nodes(connection, drupal, mapping):
    nid_to_obj = load_objs_from_database(
        connection, "subject_quick_guide", "node--guide", mapping
    )

    for nid in nid_to_obj:
        try:
            title = nid_to_obj[nid]["data"]["attributes"]["path"]["alias"].split("/")[
                3
            ][:-12]
            nid_to_obj[nid]["data"]["attributes"]["path"]["alias"] = (
                "/guides/subject/" + title
            )
        except KeyError:
            pass

    return {
        nid: drupal.post(obj)
        for nid, obj in nid_to_obj.items()
        if str(nid) not in mapping["d7_nid_to_d9_uuid"]
    }


def migrate_subject_quick_guide_fields(
    connection, drupal, nid, obj, nid_to_obj, mapping
):
    patch_ready_obj = build_obj(obj["data"]["type"], obj["data"]["id"])

    # Content Last Reviewed
    patch_ready_obj["data"]["attributes"][
        "field_content_last_reviewed"
    ] = content_reviewed(connection, nid)

    # Guide sections
    patch_ready_obj["data"]["relationships"]["field_key_resources"] = {"data": []}

    # - Field Guide Key Resources
    patch_ready_obj["data"]["relationships"]["field_key_resources"]["data"].extend(
        key_resources_to_key_resources_paragraph(
            connection, "field_key_resources", drupal, nid, nid_to_obj
        )
    )

    # Guide Type
    patch_ready_obj["data"]["attributes"]["field_guide_type"] = "Subject"

    # Subject Category
    patch_ready_obj["data"]["relationships"]["field_guide_subject_category"] = {
        "data": taxonomy_term_reference_to_taxonomy_term_reference(
            connection, "field_subject_category", nid, mapping
        )
    }

    drupal.patch(patch_ready_obj)


# Common


def load_objs_from_database(connection, bundle, d9type, mapping):
    nid_to_obj = {}
    with connection.cursor() as cursor:
        sql = "SELECT * FROM `node` WHERE `type`=%s"
        cursor.execute(sql, (bundle,))
        for row in cursor:
            obj = build_obj(d9type)
            obj["data"]["attributes"]["langcode"] = "en"
            obj["data"]["attributes"]["title"] = row["title"].strip()
            obj["data"]["attributes"]["status"] = row["status"] == 1
            obj["data"]["attributes"]["promote"] = row["promote"] == 1
            obj["data"]["attributes"]["sticky"] = row["sticky"] == 1
            obj["data"]["attributes"]["created"] = datetime.datetime.fromtimestamp(
                row["created"], tz=datetime.timezone.utc
            ).isoformat()
            alias = get_path_alias(connection, row["nid"])
            if alias:
                obj["data"]["attributes"]["path"] = {"alias": alias}
            if str(row["uid"]) not in mapping["users"]:
                print(
                    f"{row['nid']} author uid {row['uid']} is not in the mapping",
                    file=sys.stderr,
                )
            else:
                obj["data"]["relationships"]["uid"] = {
                    "data": {
                        "type": "user--user",
                        "id": mapping["users"][str(row["uid"])],
                    }
                }
            nid_to_obj[row["nid"]] = obj
    return nid_to_obj


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


def build_obj(d9type, uuid=None):
    obj = {
        "data": {
            "type": d9type,
            "attributes": {},
            "relationships": {},
        },
    }
    if uuid is not None:
        obj["data"]["id"] = uuid
    return obj


def load_objs_from_mapping(mapping, drupal):
    nid_to_obj = {}
    for nid, uuid in mapping["d7_nid_to_d9_uuid"].items():
        if nid not in mapping["d7_nid_to_d9_type"]:
            sys.exit(
                f"Error in mapping file - missing {nid} in 'd7_nid_to_d9_type' section."
            )
        obj = build_obj(mapping["d7_nid_to_d9_type"][nid], uuid)
        obj = drupal.get(obj)
        nid_to_obj[int(nid)] = obj
    return nid_to_obj


def clean_uri(uri, nid_to_obj, originating_nid):
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
            obj = nid_to_obj[int(nid)]
        except KeyError:
            print(
                f"WARNING: Unable to find D9 Node for {nid} in {originating_nid}.",
                file=sys.stderr,
            )
            obj = {"data": {"attributes": {"drupal_internal__nid": "fourohfour"}}}
        uri = "internal:/node/" + str(obj["data"]["attributes"]["drupal_internal__nid"])
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


def clean_text(value, nid_to_obj, nid):
    # Pass any links through the clean_uri function
    re.sub(
        r'href="(?P<href>[^"]+)"',
        lambda m: 'href="' + clean_uri(m.group("href"), nid_to_obj, nid) + '"',
        value,
    )
    return value


def text_with_summary_to_text_with_summary(connection, fieldname, nid, nid_to_obj):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        field.append(
            {
                "value": clean_text(row[f"{fieldname}_value"], nid_to_obj, nid),
                "summary": clean_text(row[f"{fieldname}_summary"], nid_to_obj, nid),
                "format": convert_text_format(row[f"{fieldname}_format"]),
            }
        )
    return field


def formatted_text_to_formatted_text(connection, fieldname, nid, nid_to_obj):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        field.append(
            {
                "value": clean_text(row[f"{fieldname}_value"], nid_to_obj, nid),
                "format": convert_text_format(row[f"{fieldname}_format"]),
            }
        )
    return field


def text_to_plain_text(connection, fieldname, nid):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        field.append(row[f"{fieldname}_value"])
    return field


def text_list_to_text_list(connection, fieldname, nid):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        field.append(row[f"{fieldname}_value"])
    return field


def link_to_link(connection, fieldname, nid, nid_to_obj):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        field_data = {
            "uri": clean_uri(row[f"{fieldname}_url"], nid_to_obj, nid),
            "title": row[f"{fieldname}_title"],
        }
        if field_data["uri"] == "":
            field_data["uri"] = "route:<nolink>"
        field.append(field_data)
    return field


def content_reviewed(connection, nid):
    rows = load_field_data(connection, "field_content_reviewed", nid)
    field = []
    for row in rows:
        field.append(row["field_content_reviewed_value"].strftime("%Y-%m-%d"))
    return field


def image(connection, fieldname, drupal, nid):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        path, filename = get_path_from_fid(connection, row[f"{fieldname}_fid"])
        file_resp = drupal.post_file(
            path, filename, "media", "image", "field_media_image"
        )
        media_image = build_obj("media--image")
        media_image["data"]["attributes"]["name"] = filename
        media_image["data"]["relationships"]["field_media_image"] = {
            "data": {
                "type": "file--file",
                "id": file_resp["data"]["id"],
                "meta": {
                    "alt": row[f"{fieldname}_alt"],
                },
            }
        }
        media_image = drupal.post(media_image)
        field.append(
            {
                "type": "media--image",
                "id": media_image["data"]["id"],
            }
        )
    return field


def taxonomy_term_reference_to_taxonomy_term_reference(
    connection, fieldname, nid, mapping
):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        tid = str(row[f"{fieldname}_tid"])
        field.append(
            {
                "id": mapping["d7_tid_to_d9_uuid"][tid],
                "type": mapping["d7_tid_to_d9_taxonomy_type"][tid],
            }
        )
    return field


def entity_reference_to_entity_reference(connection, fieldname, nid, nid_to_obj):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        field.append(
            {
                "id": nid_to_obj[row[f"{fieldname}_target_id"]]["data"]["id"],
                "type": nid_to_obj[row[f"{fieldname}_target_id"]]["data"]["type"],
            }
        )
    return field


def text_with_summary_to_text_area_paragraph(
    connection, fieldname, parent_field_name, drupal, nid, nid_to_obj
):
    rows = load_field_data(connection, fieldname, nid)
    field = []
    for row in rows:
        paragraph = build_obj("paragraph--text_area")
        paragraph["data"]["attributes"]["parent_id"] = nid_to_obj[nid]["data"][
            "attributes"
        ]["drupal_internal__nid"]
        paragraph["data"]["attributes"]["parent_type"] = "node"
        paragraph["data"]["attributes"]["parent_field_name"] = parent_field_name
        paragraph["data"]["attributes"]["field_text"] = {
            "value": clean_text(row[f"{fieldname}_value"], nid_to_obj, nid),
            "format": convert_text_format(row[f"{fieldname}_format"]),
        }
        paragraph = drupal.post(paragraph)
        field.append(
            {
                "type": "paragraph--text_area",
                "id": paragraph["data"]["id"],
                "meta": {
                    "target_revision_id": paragraph["data"]["attributes"][
                        "drupal_internal__revision_id"
                    ]
                },
            }
        )
    return field


def detailed_guide_section_to_accordion_paragraph(
    connection, parent_field_name, drupal, nid, nid_to_obj
):
    rows = load_field_data(connection, "field_detailed_guide_section", nid)
    field = []
    for row in rows:

        paragraph = build_obj("paragraph--accordion")
        paragraph["data"]["attributes"]["parent_id"] = nid_to_obj[nid]["data"][
            "attributes"
        ]["drupal_internal__nid"]
        paragraph["data"]["attributes"]["parent_type"] = "node"
        paragraph["data"]["attributes"]["parent_field_name"] = parent_field_name

        paragraph["data"]["attributes"]["field_accordion_title"] = text_to_plain_text(
            connection,
            "field_detailed_guide_section_lab",
            row["field_detailed_guide_section_value"],
        )

        paragraph["data"]["attributes"][
            "field_text"
        ] = formatted_text_to_formatted_text(
            connection,
            "field_detailed_guide_section_bla",
            row["field_detailed_guide_section_value"],
            nid_to_obj,
        )

        paragraph = drupal.post(paragraph)
        field.append(
            {
                "type": "paragraph--accordion",
                "id": paragraph["data"]["id"],
                "meta": {
                    "target_revision_id": paragraph["data"]["attributes"][
                        "drupal_internal__revision_id"
                    ]
                },
            }
        )
    return field


def subpage_to_accordion_paragraph(
    connection, parent_field_name, drupal, nid, nid_to_obj
):
    rows = load_subpage_data(connection, nid)
    field = []
    for row in rows:
        paragraph = build_obj("paragraph--accordion")
        paragraph["data"]["attributes"]["parent_id"] = nid_to_obj[nid]["data"][
            "attributes"
        ]["drupal_internal__nid"]
        paragraph["data"]["attributes"]["parent_type"] = "node"
        paragraph["data"]["attributes"]["parent_field_name"] = parent_field_name
        paragraph["data"]["attributes"]["field_accordion_title"] = row["title"]

        paragraph["data"]["attributes"][
            "field_text"
        ] = formatted_text_to_formatted_text(connection, "body", row["nid"], nid_to_obj)

        paragraph = drupal.post(paragraph)
        field.append(
            {
                "type": "paragraph--accordion",
                "id": paragraph["data"]["id"],
                "meta": {
                    "target_revision_id": paragraph["data"]["attributes"][
                        "drupal_internal__revision_id"
                    ]
                },
            }
        )
    return field


def key_resources_to_key_resources_paragraph(
    connection, parent_field_name, drupal, nid, nid_to_obj
):
    rows = load_field_data(connection, "field_key_resources", nid)
    field = []
    for row in rows:

        paragraph = build_obj("paragraph--key_resources")
        paragraph["data"]["attributes"]["parent_id"] = nid_to_obj[nid]["data"][
            "attributes"
        ]["drupal_internal__nid"]
        paragraph["data"]["attributes"]["parent_type"] = "node"
        paragraph["data"]["attributes"]["parent_field_name"] = parent_field_name

        paragraph["data"]["attributes"][
            "field_key_resource_annotation"
        ] = text_to_plain_text(
            connection,
            "field_key_resource_annotation",
            row["field_key_resources_value"],
        )

        paragraph["data"]["relationships"]["field_another_database"] = {
            "data": entity_reference_to_entity_reference(
                connection,
                "field_key_resource_databases",
                row["field_key_resources_value"],
                nid_to_obj,
            )
        }

        paragraph["data"]["attributes"]["field_key_resource_link"] = link_to_link(
            connection,
            "field_key_resource_link",
            row["field_key_resources_value"],
            nid_to_obj,
        )

        paragraph = drupal.post(paragraph)
        field.append(
            {
                "type": "paragraph--key_resources",
                "id": paragraph["data"]["id"],
                "meta": {
                    "target_revision_id": paragraph["data"]["attributes"][
                        "drupal_internal__revision_id"
                    ]
                },
            }
        )
    return field


def contact_service_point(connection, drupal, nid, nid_to_obj):
    rows = load_field_data(connection, "field_service_point", nid)
    from_library_paragraphs = []
    for row in rows:
        paragraph = build_obj("paragraph--from_library")
        paragraph["data"]["attributes"]["parent_id"] = nid_to_obj[nid]["data"][
            "attributes"
        ]["drupal_internal__nid"]
        paragraph["data"]["attributes"]["parent_type"] = "node"
        paragraph["data"]["attributes"][
            "parent_field_name"
        ] = "field_contact_service_point"
        paragraph["data"]["relationships"]["field_reusable_paragraph"] = {
            "data": {
                "id": nid_to_obj[row["field_service_point_target_id"]]["data"]["id"],
                "type": nid_to_obj[row["field_service_point_target_id"]]["data"][
                    "type"
                ],
            }
        }
        paragraph = drupal.post(paragraph)
        from_library_paragraphs.append(
            {
                "type": paragraph["data"]["type"],
                "id": paragraph["data"]["id"],
                "meta": {
                    "target_revision_id": paragraph["data"]["attributes"][
                        "drupal_internal__revision_id"
                    ]
                },
            }
        )
    return from_library_paragraphs


def load_subpage_data(connection, nid):
    with connection.cursor() as cursor:
        sql = (
            "SELECT `node`.`nid`, `node`.`title` "
            "FROM `book` "
            "LEFT JOIN `menu_links` ON `book`.`mlid` = `menu_links`.`mlid` "
            "LEFT JOIN `node` ON `book`.`nid` = `node`.`nid` "
            "WHERE `book`.`bid`=%s AND `book`.`nid`!=%s "
            "ORDER BY `menu_links`.`weight`"
        )
        cursor.execute(sql, (nid, nid))
        return cursor.fetchall()


def load_field_data(connection, fieldname, nid):
    with connection.cursor() as cursor:
        sql = (
            f"SELECT * FROM `field_data_{fieldname}` "
            "WHERE `entity_id` = %s "
            "ORDER BY `delta`"
        )
        cursor.execute(sql, (nid,))
        return cursor.fetchall()


def load_type(connection, nid):
    with connection.cursor() as cursor:
        sql = "SELECT `type` FROM `node` WHERE `nid` = %s"
        cursor.execute(sql, (nid,))
        return cursor.fetchone()["type"]


def convert_text_format(textformat):
    if textformat == "1":
        return "basic_html"
    elif textformat == "2":
        return "full_html"
    elif textformat == "3":
        return "full_html"
    else:
        return "plain_text"


def node_newer_than_cutoff(connection, nid, cutoff):
    with connection.cursor() as cursor:
        sql = "SELECT changed FROM `node` WHERE `nid`=%s"
        cursor.execute(sql, (nid,))
        row = cursor.fetchone()
        if row is not None and datetime.datetime.fromtimestamp(row["changed"]) > cutoff:
            return True
    return False


if __name__ == "__main__":
    cli()
