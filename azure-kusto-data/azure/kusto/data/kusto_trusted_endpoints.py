import copy
from typing import Callable, List, Dict, Union
from urllib.parse import urlparse

from azure.kusto.data.helpers import get_string_tail_lower_case
from azure.kusto.data.security import _is_local_address
from .exceptions import KustoClientInvalidConnectionStringException
from .helpers import load_bundled_json


class MatchRule:
    def __init__(self, suffix, exact):
        self.suffix = suffix.lower()
        self.exact = exact


class FastSuffixMatcher:
    def __init__(self, rules: List[MatchRule]):
        self._suffix_length = min(len(rule.suffix) for rule in rules)
        _processed_rules: Dict[str, List] = {}
        for rule in rules:
            suffix = get_string_tail_lower_case(rule.suffix, self._suffix_length)
            if suffix not in _processed_rules:
                _processed_rules[suffix] = []
            _processed_rules[suffix].append(rule)

        self.rules = _processed_rules

    def is_match(self, candidate):
        if len(candidate) < self._suffix_length:
            return False

        _match_rules = self.rules.get(get_string_tail_lower_case(candidate, self._suffix_length))
        if _match_rules:
            for rule in _match_rules:
                if candidate.lower().endswith(rule.suffix):
                    if len(candidate) == len(rule.suffix) or not rule.exact:
                        return True

        return False


def create_fast_suffix_matcher_from_existing(rules: List[MatchRule], existing: FastSuffixMatcher) -> FastSuffixMatcher:
    if existing is None or len(existing.rules) == 0:
        return FastSuffixMatcher(rules)

    if not rules:
        return existing

    return FastSuffixMatcher([*copy.deepcopy(rules), *(v for item in existing.rules.values() for v in item)])


class KustoTrustedEndpoints:
    def __init__(self):
        self._matchers = {
            k: FastSuffixMatcher(
                [*(MatchRule(suffix, False) for suffix in v["AllowedKustoSuffixes"]), *(MatchRule(hostname, True) for hostname in v["AllowedKustoHostnames"])]
            )
            for (k, v) in _well_known_kusto_endpoints_data["AllowedEndpointsByLogin"].items()
        }

        self._additional_matcher = None
        self._override_matcher = None

    def set_override_matcher(self, matcher):
        self._override_matcher = matcher

    def add_trusted_hosts(self, rules, replace):
        if rules is None or not rules:
            if replace:
                self._additional_matcher = None
            return

        self._additional_matcher = create_fast_suffix_matcher_from_existing(rules, None if replace else self._additional_matcher)

    def validate_trusted_endpoint(self, endpoint: str, login_endpoint: Union[str, Callable[[], str]]):
        """Validates that the endpoint is trusted.

        `login_endpoint` may be a callable, in which case it is only resolved if the built-in
        per-cloud allow lists actually have to be consulted. Resolving it may require contacting
        the cluster itself, so callers should pass a callable to avoid reaching out to hosts that
        can never be trusted.
        """
        hostname = urlparse(endpoint).hostname
        self.validate_hostname_is_trusted(hostname if hostname is not None else endpoint, login_endpoint)

    def validate_hostname_is_trusted(self, hostname: str, login_endpoint: Union[str, Callable[[], str]]):
        if _is_local_address(hostname):
            return
        if self._override_matcher is not None:
            if self._override_matcher(hostname):
                return

        matcher = self._additional_matcher
        if matcher is not None and matcher.is_match(hostname):
            return

        if self._override_matcher is None and any(matcher.is_match(hostname) for matcher in self._matchers.values()):
            # Only resolve the login endpoint once the hostname is known to appear in at least one
            # cloud's allow list. Resolving it may contact the cluster, so doing it first would let
            # an untrusted connection string drive a request to an arbitrary host before it is
            # rejected.
            resolved_login_endpoint = login_endpoint() if callable(login_endpoint) else login_endpoint
            matcher = self._matchers.get(resolved_login_endpoint.lower())
            if matcher is not None and matcher.is_match(hostname):
                return

        raise KustoClientInvalidConnectionStringException(
            f"Can't communicate with '{hostname}' as this hostname is currently not trusted; please see https://aka.ms/kustotrustedendpoints"
        )

    def set_override_policy(self, matcher):
        self._override_matcher = matcher


_well_known_kusto_endpoints_data = load_bundled_json("wellKnownKustoEndpoints.json")
well_known_kusto_endpoints = KustoTrustedEndpoints()
