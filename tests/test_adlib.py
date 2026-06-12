import pytest

from autoingest.resources.adlib import (
    retrieve_field_name,
    retrieve_facet_list,
    group_check,
    escape_xml,
    create_grouped_data,
)


class TestRetrieveFieldName:
    def test_simple_string_field(self):
        record = {"title": ["The Great Escape"]}
        assert retrieve_field_name(record, "title") == ["The Great Escape"]

    def test_language_field(self):
        record = {
            "title": [
                {
                    "value": [{"spans": [{"text": "Le Grand Escape"}]}],
                    "@lang": "fre",
                }
            ]
        }
        assert retrieve_field_name(record, "title") == ["Le Grand Escape"]

    def test_plain_spans_text(self):
        record = {"title": [{"spans": [{"text": "The Great Escape"}]}]}
        assert retrieve_field_name(record, "title") == ["The Great Escape"]

    def test_missing_field_delegates_to_group_check(self):
        record = {
            "group_reproduction": [
                {
                    "reproduction.title": [
                        {"spans": [{"text": "Slide Copy"}]}
                    ]
                }
            ]
        }
        result = retrieve_field_name(record, "reproduction.title")
        assert isinstance(result, list)

    def test_keyerror_on_missing_returns_group_result(self):
        record = {
            "group_reproduction": [
                {"reproduction.title": [{"spans": [{"text": "Slide Copy"}]}]}
            ]
        }
        result = retrieve_field_name(record, "reproduction.title")
        assert result == ["Slide Copy"]


class TestRetrieveFacetList:
    def test_extracts_facet_values(self):
        record = {
            "adlibJSON": {
                "facetList": [
                    {
                        "values": [
                            {"title": {"spans": [{"text": "Value A"}]}},
                            {"title": {"spans": [{"text": "Value B"}]}},
                        ]
                    }
                ]
            }
        }
        result = retrieve_facet_list(record, "title")
        assert result == ["Value A", "Value B"]

    def test_single_facet(self):
        record = {
            "adlibJSON": {
                "facetList": [
                    {
                        "values": [
                            {"type": {"spans": [{"text": "Video"}]}}
                        ]
                    }
                ]
            }
        }
        result = retrieve_facet_list(record, "type")
        assert result == ["Video"]


class TestGroupCheck:
    def test_single_matching_group_returns_fieldnames(self):
        record = {
            "group_reproduction": [
                {"reproduction.title": [{"spans": [{"text": "Copy"}]}]}
            ]
        }
        result = group_check(record, "reproduction.title")
        assert result == ["Copy"]

    def test_multiple_matching_groups_returns_all_vals(self):
        record = {
            "group_a": [{"field_x": "val1"}],
            "group_b": [{"field_x": "val2"}],
        }
        result = group_check(record, "field_x")
        assert isinstance(result, list)

    def test_no_match_returns_none(self):
        record = {"group_a": [{"other": "value"}]}
        assert group_check(record, "nonexistent") is None


class TestEscapeXml:
    def test_escapes_all_special_chars(self):
        result = escape_xml('a & b < c > d " e \' f')
        assert result == "a &amp; b &lt; c &gt; d &quot; e &apos; f"

    def test_no_special_chars(self):
        assert escape_xml("hello world") == "hello world"

    def test_non_string_passthrough(self):
        assert escape_xml(123) == 123
        assert escape_xml(None) is None


class TestCreateGroupedData:
    def test_no_priref_returns_none(self):
        assert create_grouped_data("", "group", [{"k": "v"}]) is None

    def test_basic_grouped_data(self):
        result = create_grouped_data(
            "12345",
            "group_reproduction",
            [{"reproduction.title": "Slide Copy"}],
        )
        assert "<record priref='12345'>" in result
        assert "<group_reproduction>" in result
        assert "<reproduction.title><![CDATA[Slide Copy]]></reproduction.title>" in result

    def test_multiple_field_pairs(self):
        result = create_grouped_data(
            "67890",
            "group_creator",
            [
                [{"creator": "Person A"}, {"creator.date": "2020"}],
                [{"creator": "Person B"}],
            ],
        )
        assert result.count("<group_creator>") == 2

    def test_list_of_dicts_as_field_pairs(self):
        result = create_grouped_data(
            "111",
            "group_test",
            [{"key1": "val1"}, {"key2": "val2"}],
        )
        assert "<group_test>" in result
        assert "<key1><![CDATA[val1]]></key1>" in result
