from __future__ import annotations

from copy import deepcopy

from oracle_lab.public_view import REDACTED, public_view


def test_public_view_redacts_mixed_case_credentials_only_inside_api_metadata() -> None:
    value = {
        "payload": {
            "API_Response_Metadata": {
                "HTTP_HEADERS": {
                    "Set-Cookie": "session=private",
                    "sEt-CoOkIe": "session=private-with-arbitrary-case",
                    "AUTHORIZATION": "Bearer private-token",
                    "ProxyAuthorization": "Basic cHJpdmF0ZQ==",
                    "x-Api-Key": "private-api-key",
                    "Content-Type": "application/json",
                },
                "AccessToken": "private-access-token",
                "client_secret": "private-client-secret",
                "provider_request_id": "req_public_123",
                "context_hash": "a" * 64,
            },
            "authorization": "Bearer exact-oracle-material",
            "content": "  sk_live_keep-this-oracle-text\n",
            "model": "sk_live_abcdefgh123456",
            "provider": "ghp_abcdefgh123456",
            "routed_provider_name": "safe-provider",
            "reasoning": {
                "summary": "The header was Bearer keep-this-too.",
                "api_response_metadata": {"Set-Cookie": "fictional-oracle-cookie"},
            },
            "effective_sampling": {
                "max_tokens": 4096,
                "debug": "sk_live_abcdefgh123456",
            },
            "model_identity": {"routing_note": "ghp_abcdefgh123456"},
        }
    }
    original = deepcopy(value)

    result = public_view(value)

    metadata = result["payload"]["API_Response_Metadata"]
    assert metadata["HTTP_HEADERS"] == {
        "Set-Cookie": REDACTED,
        "sEt-CoOkIe": REDACTED,
        "AUTHORIZATION": REDACTED,
        "ProxyAuthorization": REDACTED,
        "x-Api-Key": REDACTED,
        "Content-Type": "application/json",
    }
    assert metadata["AccessToken"] == REDACTED
    assert metadata["client_secret"] == REDACTED
    assert metadata["provider_request_id"] == "req_public_123"
    assert metadata["context_hash"] == "a" * 64
    assert result["payload"]["authorization"] == "Bearer exact-oracle-material"
    assert result["payload"]["content"] == "  sk_live_keep-this-oracle-text\n"
    assert result["payload"]["model"] == REDACTED
    assert result["payload"]["provider"] == REDACTED
    assert result["payload"]["routed_provider_name"] == "safe-provider"
    assert result["payload"]["reasoning"] == {
        "summary": "The header was Bearer keep-this-too.",
        "api_response_metadata": {"Set-Cookie": "fictional-oracle-cookie"},
    }
    assert result["payload"]["effective_sampling"] == {
        "max_tokens": 4096,
        "debug": REDACTED,
    }
    assert result["payload"]["model_identity"] == {"routing_note": REDACTED}
    assert value == original


def test_public_view_scans_secret_like_values_under_innocuous_metadata_keys() -> None:
    value = {
        "api_response_metadata": {
            "diagnostic": "provider reflected sk_live_abcdefgh123456",
            "mirror": "ghp_abcdefgh123456",
            "authorization_value": "Bearer abc.def.ghi",
            "cloud_diagnostic": "AKIA1234567890ABCDEF",
            "jwt_diagnostic": "eyJabcde.eyJfghij.signature123",
            "ordinary_id": "response-12345",
            "sha256": "0123456789abcdef" * 4,
            "max_tokens": 4096,
            "min_tokens": 1,
            "stop_tokens": [1, 2],
            "max_completion_tokens": 2048,
            "usage": {"prompt_tokens": 12, "completion_tokens": 34},
            "tokenizer": "provider-default",
            "session_token": "opaque-private-session-token",
            "cookie_value": "opaque-private-cookie",
            "secret_value": "opaque-private-secret",
            "api_key_value": "opaque-private-api-key",
            "session_token_value": "opaque-private-token",
            "aws_session_token": "opaque-private-aws-token",
            "oauth_refresh_token": "opaque-private-oauth-token",
            "x_amz_security_token": "opaque-private-amz-token",
            "aws_secret_access_key": "opaque-private-access-key",
        },
        "diagnostic": "provider reflected sk_live_abcdefgh123456",
    }

    result = public_view(value)

    assert result["api_response_metadata"] == {
        "diagnostic": REDACTED,
        "mirror": REDACTED,
        "authorization_value": REDACTED,
        "cloud_diagnostic": REDACTED,
        "jwt_diagnostic": REDACTED,
        "ordinary_id": "response-12345",
        "sha256": "0123456789abcdef" * 4,
        "max_tokens": 4096,
        "min_tokens": 1,
        "stop_tokens": [1, 2],
        "max_completion_tokens": 2048,
        "usage": {"prompt_tokens": 12, "completion_tokens": 34},
        "tokenizer": "provider-default",
        "session_token": REDACTED,
        "cookie_value": REDACTED,
        "secret_value": REDACTED,
        "api_key_value": REDACTED,
        "session_token_value": REDACTED,
        "aws_session_token": REDACTED,
        "oauth_refresh_token": REDACTED,
        "x_amz_security_token": REDACTED,
        "aws_secret_access_key": REDACTED,
    }
    assert result["diagnostic"] == "provider reflected sk_live_abcdefgh123456"


def test_public_view_preserves_sequence_shapes_and_is_idempotent() -> None:
    value = {
        "api_response_metadata": {
            "items": [
                "safe",
                ("Basic cHJpdmF0ZQ==", {"Cookie": "session=private"}),
            ],
        }
    }

    result = public_view(value)

    items = result["api_response_metadata"]["items"]
    assert isinstance(items, list)
    assert isinstance(items[1], tuple)
    assert items == ["safe", (REDACTED, {"Cookie": REDACTED})]
    assert result is not value
    assert result["api_response_metadata"] is not value["api_response_metadata"]
    assert public_view(result) == result
