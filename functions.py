#!/usr/bin/env python3
"""Module to provide utility functions for battery aggregation."""

import sys
import logging


class Functions:
    """Collection of utility functions for battery calculations and management."""

    # Exception safe max and min (attention, not working with dictionaries)
    # another option: res = [i for i in test_list if i is not None]

    def _max(self, x):
        try:
            return max(x)
        except (ValueError, TypeError):
            return None

    def _min(self, x):
        try:
            return min(x)
        except (ValueError, TypeError):
            return None

    # Interpolate f(x) if given lists Y = f(X)

    def _interpolate(self, x_values, y_values, x):
        """Interpolate a value from two lists representing a function.
        
        Args:
            x_values: List of x coordinates
            y_values: List of y coordinates (y = f(x))
            x: The x value to interpolate
            
        Returns:
            Interpolated y value at position x
        """
        if len(x_values) != len(y_values):
            logging.error("Both lists must have the same length. Exiting.")
            sys.exit()
            
        _len = len(x_values)
        if x <= x_values[0]:
            return y_values[0]
            
        if x >= x_values[_len - 1]:
            return y_values[_len - 1]
            
        for i in range(_len - 1):
            if x <= x_values[i + 1]:
                return y_values[i] + (y_values[i + 1] - y_values[i]) / (x_values[i + 1] - x_values[i]) * (x - x_values[i])


################
# test program #
################

def main():
    """Test function demonstrating interpolation with settings values."""
    # Import settings here to avoid circular imports
    import settings as s

    fn = Functions()
    for x in range(0, 251):
        x_val = x / 100.0
        result = s.MAX_CHARGE_CURRENT * fn._interpolate(
            s.CELL_CHARGE_LIMITING_VOLTAGE,
            s.CELL_CHARGE_LIMITED_CURRENT,
            x_val,
        )
        print(f"{x_val:.2f} {result:.0f}")


if __name__ == "__main__":
    main()
