#!/usr/bin/env python3

import sys
import logging


class Functions:

    # Exception safe max and min (attention, not working with dictionaries)
    # another option: res = [i for i in test_list if i is not None]

    def _max(self, x):
        """Find maximum value in an iterable safely.

        Args:
            x: Iterable to find the maximum value in

        Returns:
            The maximum value in the iterable, or None if the iterable is empty or contains non-comparable items
        """
        try:
            return max(x)
        except (TypeError, ValueError):
            return None

    def _min(self, x):
        """Find minimum value in an iterable safely.

        Args:
            x: Iterable to find the minimum value in

        Returns:
            The minimum value in the iterable, or None if the iterable is empty or contains non-comparable items
        """
        try:
            return min(x)
        except (TypeError, ValueError):
            return None

    def _interpolate(self, X, Y, x):
        """Interpolate f(x) from given lists Y = f(X).

        Performs linear interpolation between data points to find the value at x.

        Args:
            X: List of x-coordinates in ascending order
            Y: List of y-coordinates corresponding to X values
            x: The x-value to interpolate at

        Returns:
            The interpolated y-value at position x

        Raises:
            SystemExit: If X and Y lists have different lengths
        """
        if len(X) == len(Y):
            _len = len(X)
            if x <= X[0]:
                return Y[0]
            elif x >= X[_len - 1]:
                return Y[_len - 1]
            else:
                for i in range(_len - 1):
                    if x <= X[i + 1]:
                        return Y[i] + (Y[i + 1] - Y[i]) / (X[i + 1] - X[i]) * (x - X[i])
        else:
            logging.error(f"Both lists must have the same length. Exiting.")
            sys.exit()


################
# test program #
################


def main():
    """Test the Functions class by printing interpolated values.

    Prints a table of interpolated charge current values for cell voltages
    from 0 to 2.5V in 0.01V steps.
    """
    import settings as s

    fn = Functions()
    for x in range(0, 251):
        print(
            f"{x/100.0:.2f} {s.MAX_CHARGE_CURRENT * fn._interpolate(s.CELL_CHARGE_LIMITING_VOLTAGE, s.CELL_CHARGE_LIMITED_CURRENT, x/100.0):.0f}"
        )


if __name__ == "__main__":
    main()
