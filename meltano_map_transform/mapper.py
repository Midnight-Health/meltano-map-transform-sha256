"""A sample inline mapper app."""

from __future__ import annotations

import datetime
import json
import logging
from typing import TYPE_CHECKING

import simpleeval  # type: ignore[import-untyped]

import singer_sdk.typing as th
from singer_sdk import _singerlib as singer
from singer_sdk.exceptions import StreamMapConfigError
from singer_sdk.helpers._util import utc_now
from singer_sdk.mapper import PluginMapper, CustomStreamMap, md5, RemoveRecordTransform, MAPPER_SOURCE_OPTION, MAPPER_ALIAS_OPTION, NULL_STRING
from singer_sdk.mapper_base import InlineMapper

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import PurePath


def sha256(value: str) -> str:
    """Return the SHA256 hash of the input value."""
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class CustomStreamMapWithSHA256(CustomStreamMap):
    """A custom stream map that hashes the stream name with SHA256."""

    @property
    def functions(self) -> dict[str, t.Callable]:
        """Get available transformation functions.

        Returns:
            Functions which should be available for expression evaluation.
        """
        funcs: dict[str, t.Any] = simpleeval.DEFAULT_FUNCTIONS.copy()
        funcs["md5"] = md5
        funcs["datetime"] = datetime
        funcs["bool"] = bool
        funcs["json"] = json
        funcs["sha256"] = sha256
        return funcs


class PluginMapperWithSHA256(PluginMapper):
    def register_raw_stream_schema(  # noqa: PLR0912, C901
        self,
        stream_name: str,
        schema: dict,
        key_properties: t.Sequence[str] | None,
    ) -> None:
        """Register a new stream as described by its name and schema.

        If stream has already been registered and schema or key_properties has changed,
        the older registration will be removed and replaced with new, updated mappings.

        Args:
            stream_name: The stream name.
            schema: The schema definition for the stream.
            key_properties: The key properties of the stream.

        Raises:
            StreamMapConfigError: If the configuration is invalid.
        """
        if stream_name in self.stream_maps:
            primary_mapper = self.stream_maps[stream_name][0]
            if (
                isinstance(primary_mapper, self.default_mapper_type)
                and primary_mapper.raw_schema == schema
                and primary_mapper.raw_key_properties == key_properties
            ):
                return

            # Unload/reset stream maps if schema or key properties have changed.
            self.stream_maps.pop(stream_name)

        if stream_name not in self.stream_maps:
            # The 0th mapper should be the same-named treatment.
            # Additional items may be added for aliasing or multi projections.
            self.stream_maps[stream_name] = [
                self.default_mapper_type(
                    stream_name,
                    schema,
                    key_properties,
                    flattening_options=self.flattening_options,
                ),
            ]

        for stream_map_key, stream_map_val in self.stream_maps_dict.items():
            stream_def = (
                stream_map_val.copy()
                if isinstance(stream_map_val, dict)
                else stream_map_val
            )
            source_stream: str = stream_map_key
            stream_alias: str = stream_map_key

            is_source_stream_primary = True
            if isinstance(stream_def, dict):
                if MAPPER_SOURCE_OPTION in stream_def:
                    # <alias>: __source__: <source>
                    source_stream = stream_def.pop(MAPPER_SOURCE_OPTION)
                    is_source_stream_primary = False
                elif MAPPER_ALIAS_OPTION in stream_def:
                    # <source>: __alias__: <alias>
                    stream_alias = stream_def.pop(MAPPER_ALIAS_OPTION)
                    stream_alias = PluginMapper._eval_stream(stream_alias, stream_name)

            if stream_name == source_stream:
                # Exact match
                pass
            elif fnmatch.fnmatch(stream_name, source_stream):
                # Wildcard match
                if stream_alias == source_stream:
                    stream_alias = stream_name
                source_stream = stream_name
            else:
                continue

            mapper: CustomStreamMapWithSHA256 | RemoveRecordTransform

            if isinstance(stream_def, dict):
                mapper = CustomStreamMapWithSHA256(
                    stream_alias=stream_alias,
                    map_transform=stream_def,
                    map_config=self.map_config,
                    faker_config=self.faker_config,
                    raw_schema=schema,
                    key_properties=key_properties,
                    flattening_options=self.flattening_options,
                )
            elif stream_def is None or (stream_def == NULL_STRING):
                mapper = RemoveRecordTransform(
                    stream_alias=stream_alias,
                    raw_schema=schema,
                    key_properties=None,
                    flattening_options=self.flattening_options,
                )
                logging.info("Set null transform as default for '%s'", stream_name)

            elif isinstance(stream_def, str):
                # Non-NULL string values are not currently supported
                msg = f"Option '{stream_map_key}:{stream_def}' is not expected."
                raise StreamMapConfigError(msg)

            else:
                msg = (  # type: ignore[unreachable]
                    f"Unexpected stream definition type. Expected str, dict, or None. "
                    f"Got '{type(stream_def).__name__}'."
                )
                raise StreamMapConfigError(msg)

            if is_source_stream_primary:
                # Zero-th mapper should be the same-keyed mapper.
                # Override the default mapper with this custom map.
                self.stream_maps[source_stream][0] = mapper
            else:
                # Additional mappers for aliasing and multi-projection:
                self.stream_maps[source_stream].append(mapper)


class StreamTransform(InlineMapper):
    """A map transformer which implements the Stream Maps capability."""

    name = "meltano-map-transformer"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "stream_maps",
            th.ObjectType(
                additional_properties=th.CustomType(
                    {
                        "type": ["object", "string", "null"],
                        "properties": {
                            "__filter__": {"type": ["string", "null"]},
                            "__source__": {"type": ["string", "null"]},
                            "__alias__": {"type": ["string", "null"]},
                            "__else__": {
                                "type": ["string", "null"],
                                "enum": [None, "__NULL__"],
                            },
                            "__key_properties__": {
                                "type": ["array", "null"],
                                "items": {"type": "string"},
                            },
                        },
                        "additionalProperties": {"type": ["string", "null"]},
                    },
                ),
            ),
            required=True,
            description="Stream maps",
        ),
        th.Property(
            "flattening_enabled",
            th.BooleanType(),
            description=(
                "'True' to enable schema flattening and automatically expand nested "
                "properties."
            ),
        ),
        th.Property(
            "flattening_max_depth",
            th.IntegerType(),
            description="The max depth to flatten schemas.",
        ),
    ).to_dict()

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Create a new inline mapper.

        Args:
            config: Mapper configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        self.mapper = PluginMapperWithSHA256(plugin_config=dict(self.config), logger=self.logger)

    def map_schema_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Map a schema message according to config.

        Args:
            message_dict: A SCHEMA message JSON dictionary.

        Yields:
            Transformed schema messages.
        """
        self._assert_line_requires(message_dict, requires={"stream", "schema"})

        stream_id: str = message_dict["stream"]
        self.mapper.register_raw_stream_schema(
            stream_id,
            message_dict["schema"],
            message_dict.get("key_properties", []),
        )
        for stream_map in self.mapper.stream_maps[stream_id]:
            yield singer.SchemaMessage(
                stream_map.stream_alias,
                stream_map.transformed_schema,
                stream_map.transformed_key_properties,
                message_dict.get("bookmark_keys", []),
            )

    def map_record_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Map a record message according to config.

        Args:
            message_dict: A RECORD message JSON dictionary.

        Yields:
            Transformed record messages.
        """
        self._assert_line_requires(message_dict, requires={"stream", "record"})

        stream_id: str = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_id]:
            mapped_record = stream_map.transform(message_dict["record"])
            if mapped_record is not None:
                yield singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=message_dict.get("version"),
                    time_extracted=utc_now(),
                )

    def map_state_message(self, message_dict: dict) -> list[singer.Message]:
        """Do nothing to the message.

        Args:
            message_dict: A STATE message JSON dictionary.

        Returns:
            The same state message
        """
        return [singer.StateMessage(value=message_dict["value"])]

    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Duplicate the message or alias the stream name as defined in configuration.

        Args:
            message_dict: An ACTIVATE_VERSION message JSON dictionary.

        Yields:
            An ACTIVATE_VERSION for each duplicated or aliased stream.
        """
        self._assert_line_requires(message_dict, requires={"stream", "version"})

        stream_id: str = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_id]:
            yield singer.ActivateVersionMessage(
                stream=stream_map.stream_alias,
                version=message_dict["version"],
            )
