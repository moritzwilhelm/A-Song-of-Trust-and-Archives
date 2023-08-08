import re
from json import JSONEncoder, JSONDecoder

from requests.structures import CaseInsensitiveDict

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


def normalize_headers(headers: Headers, origin: str | None = None) -> Headers:
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


def classify_headers(headers: Headers, origin: str | None = None) -> Headers:
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


# ----------------------------------------------------------------------------
# X-Frame-Options

def normalize_xfo(value: str) -> str:
    tokens = [token.strip() for token in value.lower().split(',')]
    tokens.sort()
    return ','.join(tokens)


# ASSUMPTION: modern browsers do not support ALLOW-FROM => only SAMEORIGIN and DENY are considered
def classify_xfo(value: str) -> XFO:
    value = normalize_xfo(value)
    if value == 'sameorigin':
        return XFO.SELF
    if value == 'deny':
        return XFO.NONE
    else:
        return XFO.UNSAFE


# ----------------------------------------------------------------------------
# Content-Security-Policy

def normalize_csp(value: str) -> str:
    value = re.sub(r"'nonce-[^']*'", "'nonce-VALUE'", value, flags=re.IGNORECASE)
    value = re.sub(r"'sha(256|384|512)-[^']*'", r"'sha\1-VALUE'", value, flags=re.IGNORECASE)
    value = re.sub(r"report-(uri|to)[^;,]*", r"report-\1 REPORT_URI", value, flags=re.IGNORECASE)

    normalized_policies = []
    for policy in value.lower().split(','):
        normalized_policy = []

        for directive in policy.strip().split(';'):
            directive_name, *tokens = [token.strip() for token in directive.strip().split(' ')]
            if directive_name == '':
                continue
            normalized_policy.append(' '.join([directive_name, *sorted(tokens)]))

        normalized_policy.sort()
        normalized_policies.append(';'.join(normalized_policy))

    normalized_policies.sort()
    return ','.join(normalized_policies)


class CSP(dict):
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
            directive_name, *tokens = [token.strip() for token in directive.strip().split(' ')]
            if directive_name == '':
                continue
            csp.add_directive(directive_name.lower(), {*tokens})
        policies.append(csp)
    return policies


def classify_framing_control(origin: str, directive: set[str]) -> CspFA:
    if directive == {"'none'"} or len(directive) == 0:
        return CspFA.NONE
    secure_origin = origin.replace('http://', 'https://')
    domain = re.sub(r"http(s)://", '', origin, flags=re.IGNORECASE)
    self_expressions = {'self', origin, origin + '/', secure_origin, secure_origin + '/', domain, domain + '/'}
    if len(directive) > 0 and all(source in self_expressions for source in directive):
        return CspFA.SELF
    unsafe_expressions = {'*', 'http:', 'http://', 'http://*', 'https:', 'https://', 'https://*'}
    if any(source in unsafe_expressions for source in directive):
        return CspFA.UNSAFE
    return CspFA.CONSTRAINED


def is_unsafe_inline_active(directive: set[str]) -> bool:
    allow_all_inline = False
    for source in directive:
        r = r"^('nonce-[A-Za-z0-9+/\-_]+={0,2}'|'sha(256|384|512)-[A-Za-z0-9+/\-_]+={0,2}'|'strict-dynamic')$"
        if re.search(r, source, flags=re.IGNORECASE):
            return False
        if re.match(r"^'unsafe-inline'$", source, flags=re.IGNORECASE):
            allow_all_inline = True
    return allow_all_inline


def classify_xss_mitigation(csp: CSP) -> CspXSS:
    unsafe_expressions = {'*', 'http:', 'http://', 'http://*', 'https:', 'https://', 'https://*', 'data:'}
    directive = csp['script-src'] if "script-src" in csp else csp.get('default-src', None)
    if directive is None or is_unsafe_inline_active(directive):
        return CspXSS.UNSAFE
    if "'strict-dynamic'" not in directive and directive & unsafe_expressions:
        return CspXSS.UNSAFE
    return CspXSS.SAFE


def classify_policy(policy: CSP, origin: str) -> dict[str, CspFA | CspXSS | CspTLS]:
    res = {'FA': CspFA.UNSAFE, 'XSS': CspXSS.UNSAFE, 'TLS': CspTLS.UNSAFE}
    if 'frame-ancestors' in policy:
        res['FA'] = classify_framing_control(origin, policy['frame-ancestors'])
    if 'script-src' in policy or 'default-src' in policy:
        res['XSS'] = classify_xss_mitigation(policy)
    if 'upgrade-insecure-requests' in policy or 'block-all-mixed-content' in policy:
        res['TLS'] = CspTLS.ENABLED
    return res


def classify_csp(value: str, origin: str) -> tuple[CspFA, CspXSS, CspTLS]:
    res = {'FA': CspFA.UNSAFE, 'XSS': CspXSS.UNSAFE, 'TLS': CspTLS.UNSAFE}
    for policy in parse_csp(value):
        for use_case, classified_value in classify_policy(policy, origin).items():
            res[use_case] = max_enum(res[use_case], classified_value)
    return res['FA'], res['XSS'], res['TLS']


# ----------------------------------------------------------------------------
# Strict-Transport-Security

# according to RFC 6797 only the first HSTS header is considered
def normalize_hsts(value: str) -> str:
    value = value.lower().split(',')[0]
    tokens = [token.strip() for token in value.split(';')]
    tokens.sort()
    return ';'.join(tokens)


def classify_hsts_age(max_age: int) -> HSTSAge:
    if max_age is None:
        return HSTSAge.UNSAFE
    elif max_age <= 0:
        return HSTSAge.DISABLE
    elif max_age < 24 * 60 * 60 * 365:
        return HSTSAge.LOW
    else:
        return HSTSAge.BIG


def classify_hsts(value: str) -> tuple[HSTSAge, HSTSSub, HSTSPreload]:
    preload = False
    include_sub_domains = False
    max_age = None
    seen_directives = set()

    for token in normalize_hsts(value).split(';'):
        directive = token.split('=')[0]
        if directive in seen_directives:
            # all directives MUST appear only once (https://datatracker.ietf.org/doc/html/rfc6797#section-6.1)
            return (HSTSAge.UNSAFE, HSTSSub.UNSAFE, HSTSPreload.ABSENT)

        if token == 'preload':
            preload = True
        elif token == 'includesubdomains':
            include_sub_domains = True
        elif directive == 'max-age':
            try:
                max_age = int(token.split('=')[1].strip('"'))
            except (ValueError, IndexError):
                return (HSTSAge.UNSAFE, HSTSSub.UNSAFE, HSTSPreload.ABSENT)

        seen_directives.add(directive)

    if max_age is None:
        # max-age directive is REQUIRED (https://datatracker.ietf.org/doc/html/rfc6797#section-6.1)
        return (HSTSAge.UNSAFE, HSTSSub.UNSAFE, HSTSPreload.ABSENT)

    classified_max_age = classify_hsts_age(max_age)

    return (classified_max_age,
            # includeSubDomains directive's presence is ignored when max-age is zero
            HSTSSub(include_sub_domains and classified_max_age != HSTSAge.DISABLE),
            HSTSPreload(preload and include_sub_domains and classified_max_age == HSTSAge.BIG))


# ----------------------------------------------------------------------------
# Referrer-Policy

def normalize_referrer_policy(value: str) -> str:
    tokens = [token.strip() for token in value.lower().split(',')]
    return ','.join(tokens)


REFERRER_POLICY_VALUES = {
    '',
    'no-referrer',
    'no-referrer-when-downgrade',
    'same-origin',
    'origin',
    'strict-origin',
    'origin-when-cross-origin',
    'strict-origin-when-cross-origin',
    'unsafe-url'
}


def classify_referrer_policy(value: str) -> RP:
    policy = ''
    for token in normalize_referrer_policy(value).split(','):
        if token in REFERRER_POLICY_VALUES:
            # only consider the latest valid policy
            # https://w3c.github.io/webappsec-referrer-policy/#parse-referrer-policy-from-header
            policy = token
    return RP(policy not in ('no-referrer-when-downgrade', 'unsafe-url'))


# ----------------------------------------------------------------------------
# Permissions-Policy

def normalize_permissions_policy(value: str) -> str:
    tokens = [token.strip() for token in value.lower().split(',')]
    tokens.sort()
    return ','.join(tokens)


def classify_permissions_policy(value: str) -> str:
    return normalize_permissions_policy(value)


# ----------------------------------------------------------------------------
# Cross-Origin-Opener-Policy

def normalize_coop(value: str) -> str:
    tokens = [token.strip() for token in value.split(';')]
    return ';'.join(tokens)


def classify_coop(value: str) -> COOP:
    directive = normalize_coop(value).split(';')[0]
    return COOP(directive in ('same-origin', 'same-origin-allow-popups'))


# ----------------------------------------------------------------------------
# Cross-Origin-Resource-Policy

def normalize_corp(value: str) -> str:
    return value.strip()


def classify_corp(value: str) -> CORP:
    directive = normalize_corp(value)
    return CORP(directive in ('same-origin', 'same-site'))


# ----------------------------------------------------------------------------
# Cross-Origin-Embedder-Policy

def normalize_coep(value: str) -> str:
    tokens = [token.strip() for token in value.split(';')]
    return ';'.join(tokens)


def classify_coep(value: str) -> COEP:
    directive = normalize_coop(value).split(';')[0]
    return COEP(directive in ('require-corp', 'credentialless'))
