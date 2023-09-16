import re
from functools import cache
from json import JSONEncoder, JSONDecoder
from typing import NamedTuple

from requests.structures import CaseInsensitiveDict
from urllib3.util import parse_url

from analysis.security_enums import HSTSAge, HSTSSub, HSTSPreload, XFO, CspXSS, CspFA, CspTLS, RP, COOP, CORP, COEP, \
    max_enum

Headers = CaseInsensitiveDict


class HeadersEncoder(JSONEncoder):
    """JSONEncoder for case-insensitive header date."""

    def default(self, obj):
        if isinstance(obj, Headers):
            return dict(obj)
        return super().default(obj)


class HeadersDecoder(JSONDecoder):
    """JSONDecoder for case-insensitive header data."""

    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=Headers, *args, **kwargs)


class Origin(NamedTuple):
    protocol: str
    host: str
    port: str | None = None

    def __str__(self):
        return f"{self.protocol}://{self.host}" if self.port is None else f"{self.protocol}://{self.host}:{self.port}"


@cache
def parse_origin(url: str) -> Origin:
    """Extract the origin of a given URL."""
    parsed_url = parse_url(url)
    return Origin(parsed_url.scheme.lower(), parsed_url.host.lower(), parsed_url.port)


def normalize_headers(headers: Headers, _: Origin | None = None) -> Headers:
    return Headers({
        'Strict-Transport-Security': (
            normalize_hsts(headers['Strict-Transport-Security'])
            if 'Strict-Transport-Security' in headers else '<MISSING>'),
        'X-Frame-Options': (
            normalize_xfo(headers['X-Frame-Options'])
            if 'X-Frame-Options' in headers else '<MISSING>'),
        'Content-Security-Policy::XSS': (
            normalize_csp(headers['Content-Security-Policy'],
                          ['default-src', 'script-src'])
            if 'Content-Security-Policy' in headers else '<MISSING>'),
        'Content-Security-Policy::FA': (
            normalize_csp(headers['Content-Security-Policy'],
                          ['frame-ancestors'])
            if 'Content-Security-Policy' in headers else '<MISSING>'),
        'Content-Security-Policy::TLS': (
            normalize_csp(headers['Content-Security-Policy'],
                          ['block-all-mixed-content', 'upgrade-insecure-requests'])
            if 'Content-Security-Policy' in headers else '<MISSING>'),
        'Content-Security-Policy': (
            normalize_csp(headers['Content-Security-Policy'])
            if 'Content-Security-Policy' in headers else '<MISSING>'),
        'Permissions-Policy': (
            normalize_permissions_policy(headers['Permissions-Policy'])
            if 'Permissions-Policy' in headers else '<MISSING>'),
        'Referrer-Policy': (
            normalize_referrer_policy(headers['Referrer-Policy'])
            if 'Referrer-Policy' in headers else '<MISSING>'),
        'Cross-Origin-Opener-Policy': (
            normalize_coop(headers['Cross-Origin-Opener-Policy'])
            if 'Cross-Origin-Opener-Policy' in headers else '<MISSING>'),
        'Cross-Origin-Resource-Policy': (
            normalize_corp(headers['Cross-Origin-Resource-Policy'])
            if 'Cross-Origin-Resource-Policy' in headers else '<MISSING>'),
        'Cross-Origin-Embedder-Policy': (
            normalize_coep(headers['Cross-Origin-Embedder-Policy'])
            if 'Cross-Origin-Embedder-Policy' in headers else '<MISSING>')
    })


def classify_headers(headers: Headers, origin: Origin | None = None) -> Headers:
    return Headers({
        'Strict-Transport-Security': classify_hsts(headers.get('Strict-Transport-Security', '')),
        'X-Frame-Options': classify_xfo(headers.get('X-Frame-Options', '')),
        'Content-Security-Policy::XSS': classify_csp_xss(headers.get('Content-Security-Policy', '')),
        'Content-Security-Policy::FA': classify_csp_fa(headers.get('Content-Security-Policy', ''), origin),
        'Content-Security-Policy::TLS': classify_csp_tls(headers.get('Content-Security-Policy', '')),
        'Content-Security-Policy': classify_csp(headers.get('Content-Security-Policy', ''), origin),
        'Permissions-Policy': classify_permissions_policy(headers.get('Permissions-Policy', ''), origin),
        'Referrer-Policy': classify_referrer_policy(headers.get('Referrer-Policy', '')),
        'Cross-Origin-Opener-Policy': classify_coop(headers.get('Cross-Origin-Opener-Policy', '')),
        'Cross-Origin-Resource-Policy': classify_corp(headers.get('Cross-Origin-Resource-Policy', '')),
        'Cross-Origin-Embedder-Policy': classify_coep(headers.get('Cross-Origin-Embedder-Policy', ''))
    })


# ----------------------------------------------------------------------------
# Strict-Transport-Security

def normalize_hsts(value: str) -> str:
    # according to RFC 6797 only the first HSTS header is considered
    value = value.lower().split(',')[0]
    tokens = sorted(token.strip() for token in value.split(';'))
    return ';'.join(tokens)


def classify_hsts_age(max_age: int | None) -> HSTSAge:
    if max_age is None:
        return HSTSAge.ABSENT
    elif max_age <= 0:
        return HSTSAge.DISABLED
    elif max_age < 60 * 60 * 24 * 365:
        return HSTSAge.LOW
    else:
        return HSTSAge.BIG


def classify_hsts(value: str) -> tuple[HSTSAge, HSTSSub, HSTSPreload]:
    max_age = None
    include_sub_domains = False
    preload = False
    seen_directives = set()

    for token in normalize_hsts(value).split(';'):
        directive = token.split('=')[0]
        if directive in seen_directives:
            # all directives MUST appear only once (https://datatracker.ietf.org/doc/html/rfc6797#section-6.1)
            return HSTSAge.ABSENT, HSTSSub.ABSENT, HSTSPreload.ABSENT

        match directive:
            case 'max-age':
                if (max_age_match := re.match(r'max-age=("?)(\d+)\1$', token)) is None:
                    return HSTSAge.ABSENT, HSTSSub.ABSENT, HSTSPreload.ABSENT
                max_age = int(max_age_match.group(2))
            case 'includesubdomains':
                include_sub_domains = True
            case 'preload':
                preload = True

        seen_directives.add(directive)

    return (classified_max_age := classify_hsts_age(max_age),
            # includeSubDomains directive's presence is ignored when max-age is absent or zero
            HSTSSub(include_sub_domains and classified_max_age not in (HSTSAge.ABSENT, HSTSAge.DISABLED)),
            HSTSPreload(preload and include_sub_domains and classified_max_age is HSTSAge.BIG))


# ----------------------------------------------------------------------------
# X-Frame-Options

def normalize_xfo(value: str) -> str:
    tokens = sorted(token.strip() for token in value.lower().split(','))
    return ','.join(tokens)


# modern browsers do not support ALLOW-FROM => only SAMEORIGIN and DENY are considered
def classify_xfo(value: str) -> XFO:
    match normalize_xfo(value):
        case 'deny':
            return XFO.DENY
        case 'sameorigin':
            return XFO.SAMEORIGIN
        case _:
            return XFO.UNSAFE


# ----------------------------------------------------------------------------
# Content-Security-Policy

def normalize_csp(value: str, valid_directives: list[str] = None) -> str:
    value = re.sub(r"'nonce-[A-Za-z0-9+/\-_]+={0,2}'", "'nonce-VALUE'", value, flags=re.IGNORECASE)
    value = re.sub(r"'sha(256|384|512)-[A-Za-z0-9+/\-_]+={0,2}'", r"'sha\1-VALUE'", value, flags=re.IGNORECASE)
    value = re.sub(r"report-(uri|to)[^;,]*", r"report-\1 REPORT_URI", value, flags=re.IGNORECASE)

    normalized_policies = []
    for policy in value.lower().split(','):
        normalized_policy = []

        for directive in policy.strip().split(';'):
            directive_name, *tokens = [token.strip() for token in directive.strip().split()] or ['']
            if directive_name == '' or valid_directives is not None and directive_name not in valid_directives:
                continue
            normalized_policy.append(' '.join([directive_name, *sorted(tokens)]))

        normalized_policy.sort()
        normalized_policies.append(';'.join(normalized_policy))

    normalized_policies.sort()
    return ','.join(normalized_policies)


class CSP(CaseInsensitiveDict):
    def add_directive(self, name: str, values: set[str]) -> bool:
        if name in self:
            return False

        self[name] = values
        return True


@cache
def parse_csp(value: str) -> list[CSP]:
    policies = []
    for policy in value.strip().split(','):
        csp = CSP()
        for directive in policy.strip().split(';'):
            directive_name, *tokens = [token.strip() for token in directive.strip().split()] or ['']
            if directive_name == '':
                continue
            csp.add_directive(directive_name, {*tokens})
        policies.append(csp)
    return policies


# XSS-Mitigation
def is_unsafe_inline_active(directive: set[str]) -> bool:
    secure_expressions = {"'nonce-VALUE'", "'sha256-VALUE'", "'sha384-VALUE'", "'sha512-VALUE'", "'strict-dynamic'"}
    return "'unsafe-inline'" in directive and not directive & secure_expressions


def classify_xss_mitigation(csp: CSP) -> CspXSS:
    directive = csp.get('script-src', csp.get('default-src', None))
    if directive is None or is_unsafe_inline_active(directive):
        return CspXSS.UNSAFE

    unsafe_expressions = {'*', 'http:', 'http://', 'http://*', 'https:', 'https://', 'https://*', 'data:'}
    if directive & unsafe_expressions and "'strict-dynamic'" not in directive:
        return CspXSS.UNSAFE

    return CspXSS.SAFE


def classify_policy_xss(policy: CSP) -> CspXSS:
    return classify_xss_mitigation(policy) if 'script-src' in policy or 'default-src' in policy else CspXSS.UNSAFE


def classify_csp_xss(value: str) -> CspXSS:
    res = CspXSS.UNSAFE
    for policy in parse_csp(normalize_csp(value)):
        res = max_enum(res, classify_policy_xss(policy))
    return res


# Framing-Control
def classify_framing_control(directive: set[str], origin: Origin) -> CspFA:
    if len(directive) == 0 or directive == {"'none'"}:
        return CspFA.NONE

    secure_origin = f"https://{origin.host}" if origin.port is None else f"https://{origin.host}:{origin.port}"
    domain = origin.host if origin.port is None else f"{origin.host}:{origin.port}"
    self_expressions = {"'self'", str(origin), f"{origin}/", secure_origin, f"{secure_origin}/", domain, f"{domain}/"}
    if all(source in self_expressions for source in directive):
        return CspFA.SELF

    unsafe_expressions = {'*', 'http:', 'http://', 'http://*', 'https:', 'https://', 'https://*'}
    if any(source in unsafe_expressions for source in directive):
        return CspFA.UNSAFE

    return CspFA.CONSTRAINED


def classify_policy_fa(policy: CSP, origin: Origin) -> CspFA:
    return classify_framing_control(policy['frame-ancestors'], origin) if 'frame-ancestors' in policy else CspFA.UNSAFE


def classify_csp_fa(value: str, origin: Origin) -> CspFA:
    res = CspFA.UNSAFE
    for policy in parse_csp(normalize_csp(value)):
        res = max_enum(res, classify_policy_fa(policy, origin))
    return res


# TLS-Enforcement
def classify_policy_tls(policy: CSP) -> CspTLS:
    if 'block-all-mixed-content' in policy:
        return CspTLS.BLOCK_ALL_MIXED_CONTENT
    if 'upgrade-insecure-requests' in policy:
        return CspTLS.UPGRADE_INSECURE_REQUESTS
    else:
        return CspTLS.UNSAFE


def classify_csp_tls(value: str) -> CspTLS:
    res = CspTLS.UNSAFE
    for policy in parse_csp(normalize_csp(value)):
        res = max_enum(res, classify_policy_tls(policy))
    return res


# All use-cases
def classify_csp(value: str, origin: Origin) -> tuple[CspXSS, CspFA, CspTLS]:
    return classify_csp_xss(value), classify_csp_fa(value, origin), classify_csp_tls(value)


# ----------------------------------------------------------------------------
# Permissions-Policy

def normalize_permissions_policy(value: str) -> str:
    directives = []
    for directive in value.lower().split(','):
        match = re.match(r"([^=]+)=(\*|\((.*)\))", directive.strip())
        if match is not None:
            name, allowlist, content = match.groups()
            if allowlist != '*':
                content = set(content.strip().split())
                allowlist = f"({' '.join(sorted(content))})"

            directives.append(f"{name}={allowlist}")
    return ','.join(sorted(directives))


# ASSUMPTION: All features have a default value of *
def classify_permissions_policy(value: str, origin: Origin) -> str:
    directives = []
    for directive in value.lower().split(','):
        match = re.match(r"([^=]+)=\((.*)\)", directive.strip())
        if match is not None:
            name = match.group(1)
            content = set(match.group(2).strip().split())
            if '*' in content:
                continue

            secure_origin = f"https://{origin.host}" if origin.port is None else f"https://{origin.host}:{origin.port}"
            domain = origin.host if origin.port is None else f"{origin.host}:{origin.port}"
            self_expressions = {f'"{origin}"', f'"{origin}/"', f'"{secure_origin}"', f'"{secure_origin}/"',
                                f'"{domain}"', f'"{domain}/"'}
            if any(expression in content for expression in self_expressions):
                content -= self_expressions
                content.add('self')

            allowlist = f"({' '.join(sorted(content))})"
            directives.append(f"{name}={allowlist}")
    return ','.join(sorted(directives))


# ----------------------------------------------------------------------------
# Referrer-Policy

def normalize_referrer_policy(value: str) -> str:
    return ','.join(token.strip() for token in value.lower().split(','))


REFERRER_POLICY_VALUES = {
    'unsafe-url',
    'same-origin',
    'no-referrer',
    'no-referrer-when-downgrade',
    'origin',
    'origin-when-cross-origin',
    'strict-origin',
    'strict-origin-when-cross-origin'
}


def classify_referrer_policy(value: str) -> RP:
    policy = ''
    for token in normalize_referrer_policy(value).split(','):
        if token in REFERRER_POLICY_VALUES:
            # only consider the latest valid policy
            # https://w3c.github.io/webappsec-referrer-policy/#parse-referrer-policy-from-header
            policy = token

    match policy:
        case 'unsafe-url':
            return RP.UNSAFE_URL
        case 'same-origin':
            return RP.SAME_ORIGIN
        case 'no-referrer':
            return RP.NO_REFERRER
        case 'no-referrer-when-downgrade':
            return RP.NO_REFERRER_WHEN_DOWNGRADE
        case 'origin':
            return RP.ORIGIN
        case 'origin-when-cross-origin':
            return RP.ORIGIN_WHEN_CROSS_ORIGIN
        case 'strict-origin':
            return RP.STRICT_ORIGIN
        case 'strict-origin-when-cross-origin' | '':
            return RP.STRICT_ORIGIN_WHEN_CROSS_ORIGIN


# ----------------------------------------------------------------------------
# Cross-Origin-Opener-Policy

def normalize_coop(value: str) -> str:
    return ';'.join(token.strip() for token in value.split(';'))


def classify_coop(value: str) -> COOP:
    directive = normalize_coop(value).split(';')[0]
    match directive:
        case 'same-origin':
            return COOP.SAME_ORIGIN
        case 'same-origin-allow-popups':
            return COOP.SAME_ORIGIN_ALLOW_POPUPS
        case _:
            return COOP.UNSAFE_NONE


# ----------------------------------------------------------------------------
# Cross-Origin-Resource-Policy

def normalize_corp(value: str) -> str:
    return value.strip()


def classify_corp(value: str) -> CORP:
    directive = normalize_corp(value)

    match directive:
        case 'same-origin':
            return CORP.SAME_ORIGIN
        case 'same-site':
            return CORP.SAME_SITE
        case _:
            return CORP.CROSS_ORIGIN


# ----------------------------------------------------------------------------
# Cross-Origin-Embedder-Policy

def normalize_coep(value: str) -> str:
    return ';'.join(token.strip() for token in value.split(';'))


def classify_coep(value: str) -> COEP:
    directive = normalize_coop(value).split(';')[0]

    match directive:
        case 'credentialless':
            return COEP.CREDENTIALLESS
        case 'require-corp':
            return COEP.REQUIRE_CORP
        case _:
            return COEP.UNSAFE_NONE
