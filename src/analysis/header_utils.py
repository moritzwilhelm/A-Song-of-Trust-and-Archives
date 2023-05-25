import re
from typing import Dict, Union, Tuple, Set, List

from analysis.security_enums import XFO, CspFA, CspXSS, CspTLS, HSTSAge, HSTSSub, HSTSPreload, RP, COOP, CORP, COEP, \
    max_enum


def normalize_headers(headers: Dict, origin: str = None) -> Dict:
    return {
        'x-frame-options': normalize_xfo(
            headers['x-frame-options']) if 'x-frame-options' in headers else '<MISSING>',
        'content-security-policy': normalize_csp(
            headers['content-security-policy']) if 'content-security-policy' in headers else '<MISSING>',
        'strict-transport-security': normalize_hsts(
            headers['strict-transport-security']) if 'strict-transport-security' in headers else '<MISSING>',
        'referrer-policy': normalize_referrer_policy(
            headers['referrer-policy']) if 'referrer-policy' in headers else '<MISSING>',
        'permissions-policy': normalize_permissions_policy(
            headers['permissions-policy']) if 'permissions-policy' in headers else '<MISSING>',
        'cross-origin-opener-policy': normalize_coop(
            headers['cross-origin-opener-policy']) if 'cross-origin-opener-policy' in headers else '<MISSING>',
        'cross-origin-resource-policy': normalize_corp(
            headers['cross-origin-resource-policy']) if 'cross-origin-resource-policy' in headers else '<MISSING>',
        'cross-origin-embedder-policy': normalize_coep(
            headers['cross-origin-embedder-policy']) if 'cross-origin-embedder-policy' in headers else '<MISSING>'
    }


def classify_headers(headers: Dict, origin: str = None) -> Dict:
    return {
        'x-frame-options': classify_xfo(headers.get('x-frame-options', '')),
        'content-security-policy': classify_csp(headers.get('content-security-policy', ''), origin),
        'strict-transport-security': classify_hsts(headers.get('strict-transport-security', '')),
        'referrer-policy': classify_referrer_policy(headers.get('referrer-policy', '')),
        'permissions-policy': classify_permissions_policy(headers.get('permissions-policy', '')),
        'cross-origin-opener-policy': classify_coop(headers.get('cross-origin-opener-policy', '')),
        'cross-origin-resource-policy': classify_corp(headers.get('cross-origin-resource-policy', '')),
        'cross-origin-embedder-policy': classify_coep(headers.get('cross-origin-embedder-policy', ''))
    }


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
    def add_directive(self, name: str, values: Set) -> bool:
        if name in self:
            return False

        self[name] = values
        return True


def parse_csp(value: str) -> List[CSP]:
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


def classify_framing_control(origin: str, directive: Set) -> CspFA:
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


def is_unsafe_inline_active(directive: Set) -> bool:
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


def classify_policy(policy: CSP, origin: str) -> Dict[str, Union[CspFA, CspXSS, CspTLS]]:
    res = {'FA': CspFA.UNSAFE, 'XSS': CspXSS.UNSAFE, 'TLS': CspTLS.UNSAFE}
    if 'frame-ancestors' in policy:
        res['FA'] = classify_framing_control(origin, policy['frame-ancestors'])
    if 'script-src' in policy or 'default-src' in policy:
        res['XSS'] = classify_xss_mitigation(policy)
    if 'upgrade-insecure-requests' in policy or 'block-all-mixed-content' in policy:
        res['TLS'] = CspTLS.ENABLED
    return res


def classify_csp(value: str, origin: str) -> Tuple[CspFA, CspXSS, CspTLS]:
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


def classify_hsts(value: str) -> Tuple[HSTSAge, HSTSSub, HSTSPreload]:
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
    tokens.sort()
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
    for token in [token.strip() for token in value.lower().split(',')]:
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
    return value.lower().strip()


def classify_coop(value: str) -> COOP:
    return COOP(normalize_coop(value) not in ('unsafe-none', ''))


# ----------------------------------------------------------------------------
# Cross-Origin-Resource-Policy

def normalize_corp(value: str) -> str:
    return value.lower().strip()


def classify_corp(value: str) -> CORP:
    return CORP(normalize_corp(value) not in ('cross-origin', ''))


# ----------------------------------------------------------------------------
# Cross-Origin-Embedder-Policy

def normalize_coep(value: str) -> str:
    return value.lower().strip()


def classify_coep(value: str) -> COEP:
    return COEP(normalize_coep(value) not in ('unsafe-none', ''))
