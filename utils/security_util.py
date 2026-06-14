# Utility functions for security validation and processing

"""
A CUSIP has:

Characters 1-6: Issuer identifier
Characters 7-8: Issue identifier
Character 9: Check digit

Example: 037833100 (for Apple Inc. stock)

Calculate the check digit as follows:
For the first 8 characters:

Convert letters to numbers:
A = 10, B = 11, ..., Z = 35
Special characters:
* = 36
@ = 37
# = 38
Starting from the second character, double every other value (positions 2, 4, 6, 8).
If a doubled value is two digits, add its digits together.
Example: 24 → 2 + 4 = 6
Sum all resulting values.

Compute:

check_digit = (10 - (sum % 10)) % 10
Compare the calculated digit with the 9th character of the CUSIP. If they match, the CUSIP is valid.

"""

def cusip_is_valid(cusip):
    if len(cusip) != 9:
        return False

    def value(c):
        if c.isdigit():
            return int(c)
        if c.isalpha():
            return ord(c.upper()) - ord('A') + 10
        if c == '*':
            return 36
        if c == '@':
            return 37
        if c == '#':
            return 38
        return None

    total = 0
    for i, c in enumerate(cusip[:8]):
        v = value(c)
        if v is None:
            return False
        if i % 2 == 1:
            v *= 2
        total += (v // 10) + (v % 10)

    check_digit = (10 - (total % 10)) % 10
    return str(check_digit) == cusip[8]


# Validate ISIN (International Securities Identification Number)
# An ISIN has 12 characters:
# First 2 characters: Country code (e.g., US, GB, CA)
# Next 9 characters: National security identifier (often a CUSIP in the U.S.)
# Last character: Check digit
# Calculate the check digit using the Luhn algorithm:
# 1. Convert letters to numbers (A=10, B=11, ..., Z=35).
# 2. Starting from the rightmost digit (the check digit), double every second digit.
# 3. If doubling results in a number greater than 9, subtract 9 from it.
# 4. Sum all the digits.
# 5. The check digit is the amount needed to round the sum up to the nearest multiple of 10.
# Example: US0378331005 (Apple Inc. stock)
def isin_is_valid(isin):
    isin = isin.strip().upper()

    if len(isin) != 12:
        return False

    # Expand letters to numbers
    expanded = ""
    for c in isin[:-1]:
        if c.isdigit():
            expanded += c
        elif c.isalpha():
            expanded += str(ord(c) - ord('A') + 10)
        else:
            return False

    if not isin[-1].isdigit():
        return False

    digits = expanded + isin[-1]

    total = 0
    reverse_digits = digits[::-1]

    for i, d in enumerate(reverse_digits):
        n = int(d)
        if i % 2 == 1:
            n *= 2
            n = n // 10 + n % 10
        total += n

    return total % 10 == 0

