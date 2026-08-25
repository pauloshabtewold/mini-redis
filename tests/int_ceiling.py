"""The one input that separates isdigit() from int(), derived rather than written down.

CPython refuses string-to-integer conversion past a ceiling (the CVE-2020-10735
mitigation, 4300 digits by default). Both `-X int_max_str_digits` and
`sys.set_int_max_str_digits` move that ceiling in either direction, and `0` removes it
entirely -- so a digit run hard-coded at 4301 is an assertion about one interpreter
configuration, not about the parser. Four test modules need this run; it is owned here.
"""

import sys

INT_MAX_STR_DIGITS = sys.get_int_max_str_digits()

# empty where the ceiling is disabled: there is then no field isdigit() admits that int() refuses
OVERSIZED_DIGIT_RUN = b"9" * (INT_MAX_STR_DIGITS + 1) if INT_MAX_STR_DIGITS else b""
NO_CONVERSION_CEILING = not INT_MAX_STR_DIGITS
NO_CEILING_REASON = "the interpreter converts any digit run, so no input reaches int() past its limit"
