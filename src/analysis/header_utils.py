import re
from json import JSONEncoder, JSONDecoder
from typing import NamedTuple

from requests.structures import CaseInsensitiveDict
from urllib3.util import parse_url

from analysis.security_enums import XFO, CspFA, CspXSS, CspTLS, HSTSAge, HSTSSub, HSTSPreload, RP, COOP, CORP, COEP, \
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


def parse_origin(url: str) -> Origin:
    """Extract the origin of a given URL."""
    parsed_url = parse_url(url)
    if parsed_url.host is None:
        parsed_url = parse_url(re.sub(r"^(https?):/(?!/)", r"\1://", url))
    return Origin(parsed_url.scheme.lower(), parsed_url.host.lower(), parsed_url.port)


def normalize_headers(headers: Headers, origin: Origin | None = None) -> Headers:
    return Headers({
        'X-Frame-Options': normalize_xfo(
            headers['X-Frame-Options']) if 'X-Frame-Options' in headers else '<MISSING>',
        'Content-Security-Policy': normalize_csp(
            headers['Content-Security-Policy']) if 'Content-Security-Policy' in headers else '<MISSING>',
        'Strict-Transport-Security': normalize_hsts(
            headers['Strict-Transport-Security']) if 'Strict-Transport-Security' in headers else '<MISSING>',
        'Referrer-Policy': normalize_referrer_policy(
            headers['Referrer-Policy']) if 'Referrer-Policy' in headers else '<MISSING>',
        'Permissions-Policy': normalize_permissions_policy(
            headers['Permissions-Policy']) if 'Permissions-Policy' in headers else '<MISSING>',
        'Cross-Origin-Opener-Policy': normalize_coop(
            headers['Cross-Origin-Opener-Policy']) if 'Cross-Origin-Opener-Policy' in headers else '<MISSING>',
        'Cross-Origin-Resource-Policy': normalize_corp(
            headers['Cross-Origin-Resource-Policy']) if 'Cross-Origin-Resource-Policy' in headers else '<MISSING>',
        'Cross-Origin-Embedder-Policy': normalize_coep(
            headers['Cross-Origin-Embedder-Policy']) if 'Cross-Origin-Embedder-Policy' in headers else '<MISSING>'
    })


def classify_headers(headers: Headers, origin: Origin | None = None) -> Headers:
    return Headers({
        'X-Frame-Options': classify_xfo(headers.get('X-Frame-Options', '')),
        'Content-Security-Policy': classify_csp(headers.get('Content-Security-Policy', ''), origin),
        'Strict-Transport-Security': classify_hsts(headers.get('Strict-Transport-Security', '')),
        'Referrer-Policy': classify_referrer_policy(headers.get('Referrer-Policy', '')),
        'Permissions-Policy': classify_permissions_policy(headers.get('Permissions-Policy', '')),
        'Cross-Origin-Opener-Policy': classify_coop(headers.get('Cross-Origin-Opener-Policy', '')),
        'Cross-Origin-Resource-Policy': classify_corp(headers.get('Cross-Origin-Resource-Policy', '')),
        'Cross-Origin-Embedder-Policy': classify_coep(headers.get('Cross-Origin-Embedder-Policy', ''))
    })


def get_headers_security(headers: Headers, origin: Origin | None = None) -> Headers:
    return Headers({
        'X-Frame-Options': is_secure_xfo(headers.get('X-Frame-Options', '')),
        'Content-Security-Policy-FA': is_secure_csp_fa(headers.get('Content-Security-Policy', ''), origin),
        'Content-Security-Policy-XSS': is_secure_csp_xss(headers.get('Content-Security-Policy', ''), origin),
        'Content-Security-Policy-TLS': is_secure_csp_tls(headers.get('Content-Security-Policy', ''), origin),
        'Strict-Transport-Security': is_secure_hsts(headers.get('Strict-Transport-Security', '')),
        'Referrer-Policy': is_secure_referrer_policy(headers.get('Referrer-Policy', '')),
        'Permissions-Policy': is_secure_permissions_policy(headers.get('Permissions-Policy', '')),
        'Cross-Origin-Opener-Policy': is_secure_coop(headers.get('Cross-Origin-Opener-Policy', '')),
        'Cross-Origin-Resource-Policy': is_secure_corp(headers.get('Cross-Origin-Resource-Policy', '')),
        'Cross-Origin-Embedder-Policy': is_secure_coep(headers.get('Cross-Origin-Embedder-Policy', ''))
    })


def get_headers_security_categories() -> tuple[str]:
    return tuple(get_headers_security(Headers()).keys())


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


def is_secure_xfo(value: str) -> bool:
    return classify_xfo(value) is not XFO.UNSAFE


# ----------------------------------------------------------------------------
# Content-Security-Policy

def normalize_csp(value: str) -> str:
    value = re.sub(r"'nonce-[A-Za-z0-9+/\-_]+={0,2}'", "'nonce-VALUE'", value, flags=re.IGNORECASE)
    value = re.sub(r"'sha(256|384|512)-[A-Za-z0-9+/\-_]+={0,2}'", r"'sha\1-VALUE'", value, flags=re.IGNORECASE)
    value = re.sub(r"report-(uri|to)[^;,]*", r"report-\1 REPORT_URI", value, flags=re.IGNORECASE)

    normalized_policies = []
    for policy in value.lower().split(','):
        normalized_policy = []

        for directive in policy.strip().split(';'):
            directive_name, *tokens = [token.strip() for token in directive.strip().split()] or ['']
            if directive_name == '':
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


def classify_framing_control(directive: set[str], origin: Origin) -> CspFA:
    if len(directive) == 0 or directive == {"'none'"}:
        return CspFA.NONE

    secure_origin = f"https://{origin.host}" if origin.port is None else f"https://{origin.host}:{origin.port}"
    domain = origin.host if origin.port is None else f"{origin.host}:{origin.port}"
    self_expressions = {"'self'", origin, f"{origin}/", secure_origin, f"{secure_origin}/", domain, f"{domain}/"}
    if all(source in self_expressions for source in directive):
        return CspFA.SELF

    unsafe_expressions = {'*', 'http:', 'http://', 'http://*', 'https:', 'https://', 'https://*'}
    if any(source in unsafe_expressions for source in directive):
        return CspFA.UNSAFE

    return CspFA.CONSTRAINED


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


def classify_policy(policy: CSP, origin: Origin) -> dict[str, CspFA | CspXSS | CspTLS]:
    res = {'FA': CspFA.UNSAFE, 'XSS': CspXSS.UNSAFE, 'TLS': CspTLS.UNSAFE}
    if 'frame-ancestors' in policy:
        res['FA'] = classify_framing_control(policy['frame-ancestors'], origin)
    if 'script-src' in policy or 'default-src' in policy:
        res['XSS'] = classify_xss_mitigation(policy)
    if 'block-all-mixed-content' in policy:
        res['TLS'] = CspTLS.BLOCK_ALL_MIXED_CONTENT
    if 'upgrade-insecure-requests' in policy:
        res['TLS'] = CspTLS.UPGRADE_INSECURE_REQUESTS
    return res


def classify_csp(value: str, origin: Origin) -> tuple[CspFA, CspXSS, CspTLS]:
    res = {'FA': CspFA.UNSAFE, 'XSS': CspXSS.UNSAFE, 'TLS': CspTLS.UNSAFE}
    for policy in parse_csp(normalize_csp(value)):
        for use_case, classified_value in classify_policy(policy, origin).items():
            res[use_case] = max_enum(res[use_case], classified_value)
    return res['FA'], res['XSS'], res['TLS']


def is_secure_csp_fa(value: str, origin: Origin) -> bool:
    return classify_csp(value, origin)[0] is not CspFA.UNSAFE


def is_secure_csp_xss(value: str, origin: Origin) -> bool:
    return classify_csp(value, origin)[1] is not CspXSS.UNSAFE


def is_secure_csp_tls(value: str, origin: Origin) -> bool:
    return classify_csp(value, origin)[2] is not CspTLS.UNSAFE


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


def is_secure_hsts(value: str) -> bool:
    max_age, include_sub_domains, preload = classify_hsts(value)
    return max_age is HSTSAge.BIG and include_sub_domains is HSTSSub.ACTIVE


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


def is_secure_referrer_policy(value: str) -> bool:
    classified_value = classify_referrer_policy(value)
    return classified_value not in (RP.UNSAFE_URL, RP.NO_REFERRER_WHEN_DOWNGRADE)


# ----------------------------------------------------------------------------
# Permissions-Policy

def normalize_permissions_policy(value: str) -> str:
    tokens = sorted(token.strip() for token in value.lower().split(','))
    return ','.join(tokens)


def classify_permissions_policy(value: str) -> str:
    return normalize_permissions_policy(value)


def is_secure_permissions_policy(value: str) -> bool:
    return True


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


def is_secure_coop(value: str) -> bool:
    return classify_coop(value) is not COOP.UNSAFE_NONE


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


def is_secure_corp(value: str) -> bool:
    return classify_corp(value) is not CORP.CROSS_ORIGIN


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


def is_secure_coep(value: str) -> bool:
    return classify_coep(value) is not COEP.UNSAFE_NONE
