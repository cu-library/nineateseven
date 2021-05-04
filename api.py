# -*- coding: utf-8 -*-
"""Drupal API"""

import requests
import click
import pprint
import json


class ValidationError(Exception):
    pass


class DrupalAPI(object):

    json_accept_header = "application/vnd.api+json"
    json_content_type_header = "application/vnd.api+json"
    default_headers = {
        "Accept": json_accept_header,
        "Content-Type": json_content_type_header,
    }

    def __init__(self, target, username, password):
        self.target = target
        self.auth = (username, password)

    def _build_url(self, entity, bundle, field_or_uuid=None):
        url = self.target + "/" + entity + "/" + bundle
        if field_or_uuid is not None:
            return url + "/" + field_or_uuid
        return url

    def _validate_obj(self, obj):
        if type(obj) is not dict:
            raise ValidationError("object must be dict")
        if "data" not in obj:
            raise ValidationError("dict must have a 'data' key")
        if "type" not in obj["data"] or obj["data"]["type"] == "":
            raise ValidationError("dict must have a non-empty 'type' key")
        if len(obj["data"]["type"].split("--")) != 2:
            raise ValidationError(
                "type must have two parts, seperated by a double-dash"
            )

    def _validate_obj_id(self, obj):
        self._validate_obj(obj)
        if "id" not in obj["data"] or obj["data"]["id"] == "":
            raise ValidationError("when patching, the object must have an id")

    def _validate_resp(self, resp):
        try:
            resp.raise_for_status()
        except requests.RequestException:
            try:
                click.echo(pprint.pformat(resp.json()))
            except json.decoder.JSONDecodeError:
                click.echo(resp.text)
            raise

    def _get_entity_and_bundle(self, obj):
        return obj["data"]["type"].split("--")

    def test(self):
        resp = requests.get(
            self.target,
            headers=self.default_headers,
            auth=self.auth,
        )
        self._validate_resp(resp)
        resp_data = resp.json()
        try:
            user_uuid = resp_data["meta"]["links"]["me"]["meta"]["id"]
            if type(user_uuid) is str and len(user_uuid) == 36:
                return True
        except KeyError:
            return False

    def get(self, obj):
        self._validate_obj_id(obj)
        entity, bundle = self._get_entity_and_bundle(obj)
        resp = requests.get(
            self._build_url(entity, bundle, obj["data"]["id"]),
            headers=self.default_headers,
            auth=self.auth,
        )
        self._validate_resp(resp)
        return resp.json()

    def post(self, obj):
        self._validate_obj(obj)
        entity, bundle = self._get_entity_and_bundle(obj)
        resp = requests.post(
            self._build_url(entity, bundle),
            headers=self.default_headers,
            data=json.dumps(obj),
            auth=self.auth,
        )
        self._validate_resp(resp)
        return resp.json()

    def patch(self, obj):
        self._validate_obj_id(obj)
        entity, bundle = self._get_entity_and_bundle(obj)
        resp = requests.patch(
            self._build_url(entity, bundle, obj["data"]["id"]),
            headers=self.default_headers,
            data=json.dumps(obj),
            auth=self.auth,
        )
        self._validate_resp(resp)
        return resp.json()

    def post_file(self, path, filename, entity, bundle, field):
        headers = {
            "Accept": self.json_accept_header,
            "Content-Type": "application/octet-stream",
            "Content-Disposition": f'file; filename="{filename}"',
        }
        with open(path, "rb") as upload_file:
            resp = requests.post(
                self._build_url(entity, bundle, field),
                headers=headers,
                data=upload_file,
                auth=self.auth,
            )
        self._validate_resp(resp)
        return resp.json()
