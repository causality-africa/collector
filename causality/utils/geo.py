from typing import Optional

import pycountry


def iso3_to_iso2(iso3_code) -> Optional[str]:
    """Convert ISO3 country code to ISO2."""
    try:
        country = pycountry.countries.get(alpha_3=iso3_code)
        return country.alpha_2 if country else None
    except (KeyError, AttributeError):
        return None
