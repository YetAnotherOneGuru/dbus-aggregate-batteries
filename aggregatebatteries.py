#!/usr/bin/env python3

"""
Service to aggregate multiple serial batteries https://github.com/Louisvdw/dbus-serialbattery
to one virtual battery.

Python location on Venus:
/usr/bin/python3.8
/usr/lib/python3.8/site-packages/

References:
https://dbus.freedesktop.org/doc/dbus-python/tutorial.html
https://github.com/victronenergy/venus/wiki/dbus
https://github.com/victronenergy/velib_python
"""

import logging
import sys
import os
import platform
import re
from datetime import datetime as dt
import time as tt
from threading import Thread
from gi.repository import GLib
import dbus
import settings
from functions import Functions
from dbusmon import DbusMon

sys.path.append("/opt/victronenergy/dbus-systemcalc-py/ext/velib_python")
from vedbus import VeDbusService  # noqa: E402  # copilot change: C0413

VERSION = "3.5"

class SystemBus(dbus.bus.BusConnection):
    """SystemBus class to handle system-level DBus connections."""  # copilot change: C0115
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SYSTEM)


class SessionBus(dbus.bus.BusConnection):
    """SessionBus class to handle session-level DBus connections."""  # copilot change: C0115
    def __new__(cls):
        return dbus.bus.BusConnection.__new__(cls, dbus.bus.BusConnection.TYPE_SESSION)


def get_bus() -> dbus.bus.BusConnection:
    """Returns the appropriate DBus connection based on the environment."""  # copilot change: C0116
    return SessionBus() if "DBUS_SESSION_BUS_ADDRESS" in os.environ else SystemBus()

class DbusAggBatService:
    """Service to aggregate multiple serial batteries into one virtual battery."""  # copilot change: C0115

    def __init__(self, servicename="com.victronenergy.battery.aggregate"):
        self._fn = Functions()
        self._batteries_dict = {}
        self._multi = None
        self._mppts_list = []
        self._smart_shunt = None  # copilot change: C0103
        self._search_trials = 0  # copilot change: C0103
        self._read_trials = 0  # copilot change: C0103
        self._max_charge_voltage_old = 0  # copilot change: C0103
        self._max_charge_current_old = 0  # copilot change: C0103
        self._max_discharge_current_old = 0  # copilot change: C0103
        self._fully_discharged = False  # copilot change: C0103
        self._dbus_conn = get_bus()  # copilot change: C0103
        logging.info("### Initialise VeDbusService ")
        self._dbusservice = VeDbusService(servicename, self._dbus_conn, register=False)
        logging.info("#### Done: Init of VeDbusService ")
        self._time_old = tt.time()  # copilot change: C0103
        self._dc_feed_active = False  # copilot change: C0103
        self._balancing = 0
        self._last_balancing = 0  # copilot change: C0103
        self._dynamic_cvl = False  # copilot change: C0103
        self._log_timer = 0  # copilot change: C0103

        try:
            with open("/data/dbus-aggregate-batteries/charge", "r") as charge_file:
                self._own_charge = float(charge_file.readline().strip())
            self._own_charge_old = self._own_charge
            logging.info(
                "%s: Initial Ah read from file: %.0fAh"
                % ((dt.now()).strftime("%c"), self._own_charge)
            )
        except FileNotFoundError:  # copilot change: W0718
            logging.error(
                "%s: Charge file not found. Exiting." % (dt.now()).strftime("%c")
            )
            sys.exit()
        except ValueError:  # copilot change: W0718
            logging.error(
                "%s: Invalid value in charge file. Exiting." % (dt.now()).strftime("%c")
            )
            sys.exit()

        if settings.OWN_CHARGE_PARAMETERS:
            try:
                with open("/data/dbus-aggregate-batteries/last_balancing", "r") as last_balancing_file:
                    self._last_balancing = int(last_balancing_file.readline().strip())
                time_unbalanced = (
                    int((dt.now()).strftime("%j")) - self._last_balancing
                )
                if time_unbalanced < 0:
                    time_unbalanced += 365
                logging.info(
                    "%s: Last balancing done at the %d. day of the year"
                    % ((dt.now()).strftime("%c"), self._last_balancing)
                )
                logging.info("Batteries balanced %d days ago." % time_unbalanced)
            except FileNotFoundError:  # copilot change: W0718
                logging.error(
                    "%s: Last balancing file not found. Exiting."
                    % (dt.now()).strftime("%c")
                )
                sys.exit()
            except ValueError:  # copilot change: W0718
                logging.error(
                    "%s: Invalid value in last balancing file. Exiting."
                    % (dt.now()).strftime("%c")
                )
                sys.exit()

        self._dbusservice.add_path("/Mgmt/ProcessName", __file__)
        self._dbusservice.add_path("/Mgmt/ProcessVersion", "Python " + platform.python_version())
        self._dbusservice.add_path("/Mgmt/Connection", "Virtual")

        self._dbusservice.add_path("/DeviceInstance", 99)
        self._dbusservice.add_path("/ProductId", 0xBA44)
        self._dbusservice.add_path("/ProductName", "AggregateBatteries")
        self._dbusservice.add_path("/FirmwareVersion", VERSION)
        self._dbusservice.add_path("/HardwareVersion", VERSION)
        self._dbusservice.add_path("/Connected", 1)

        self._dbusservice.add_path(
            "/Dc/0/Voltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.2f}V".format(x),
        )
        self._dbusservice.add_path(
            "/Dc/0/Current",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.2f}A".format(x),
        )
        self._dbusservice.add_path(
            "/Dc/0/Power",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.0f}W".format(x),
        )

        self._dbusservice.add_path("/Soc", None, writeable=True)
        self._dbusservice.add_path(
            "/Capacity",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.0f}Ah".format(x),
        )
        self._dbusservice.add_path(
            "/InstalledCapacity",
            None,
            gettextcallback=lambda a, x: "{:.0f}Ah".format(x),
        )
        self._dbusservice.add_path(
            "/ConsumedAmphours", None, gettextcallback=lambda a, x: "{:.0f}Ah".format(x)
        )

        self._dbusservice.add_path("/Dc/0/Temperature", None, writeable=True)
        self._dbusservice.add_path("/System/MinCellTemperature", None, writeable=True)
        self._dbusservice.add_path("/System/MaxCellTemperature", None, writeable=True)

        self._dbusservice.add_path(
            "/System/MinCellVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.3f}V".format(x),
        )
        self._dbusservice.add_path("/System/MinVoltageCellId", None, writeable=True)
        self._dbusservice.add_path(
            "/System/MaxCellVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.3f}V".format(x),
        )
        self._dbusservice.add_path("/System/MaxVoltageCellId", None, writeable=True)
        self._dbusservice.add_path("/System/NrOfCellsPerBattery", None, writeable=True)
        self._dbusservice.add_path("/System/NrOfModulesOnline", None, writeable=True)
        self._dbusservice.add_path("/System/NrOfModulesOffline", None, writeable=True)
        self._dbusservice.add_path(
            "/System/NrOfModulesBlockingCharge", None, writeable=True
        )
        self._dbusservice.add_path(
            "/System/NrOfModulesBlockingDischarge", None, writeable=True
        )
        self._dbusservice.add_path(
            "/Voltages/Sum",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.3f}V".format(x),
        )
        self._dbusservice.add_path(
            "/Voltages/Diff",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.3f}V".format(x),
        )
        self._dbusservice.add_path("/TimeToGo", None, writeable=True)

        self._dbusservice.add_path("/Alarms/LowVoltage", None, writeable=True)
        self._dbusservice.add_path("/Alarms/HighVoltage", None, writeable=True)
        self._dbusservice.add_path("/Alarms/LowCellVoltage", None, writeable=True)
        self._dbusservice.add_path("/Alarms/LowSoc", None, writeable=True)
        self._dbusservice.add_path("/Alarms/HighChargeCurrent", None, writeable=True)
        self._dbusservice.add_path("/Alarms/HighDischargeCurrent", None, writeable=True)
        self._dbusservice.add_path("/Alarms/CellImbalance", None, writeable=True)
        self._dbusservice.add_path("/Alarms/InternalFailure", None, writeable=True)
        self._dbusservice.add_path(
            "/Alarms/HighChargeTemperature", None, writeable=True
        )
        self._dbusservice.add_path("/Alarms/LowChargeTemperature", None, writeable=True)
        self._dbusservice.add_path("/Alarms/HighTemperature", None, writeable=True)
        self._dbusservice.add_path("/Alarms/LowTemperature", None, writeable=True)
        self._dbusservice.add_path("/Alarms/BmsCable", None, writeable=True)

        self._dbusservice.add_path(
            "/Info/MaxChargeCurrent",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.1f}A".format(x),
        )
        self._dbusservice.add_path(
            "/Info/MaxDischargeCurrent",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.1f}A".format(x),
        )
        self._dbusservice.add_path(
            "/Info/MaxChargeVoltage",
            None,
            writeable=True,
            gettextcallback=lambda a, x: "{:.2f}V".format(x),
        )
        self._dbusservice.add_path("/Io/AllowToCharge", None, writeable=True)
        self._dbusservice.add_path("/Io/AllowToDischarge", None, writeable=True)
        self._dbusservice.add_path("/Io/AllowToBalance", None, writeable=True)

        logging.info("### Registering VeDbusService")
        self._dbusservice.register()
        
        x = Thread(target=self._start_monitor)
        x.start()

        GLib.timeout_add(1000, self._find_settings)

    def _start_monitor(self):
        """Starts the battery monitor in a separate thread."""  # copilot change: C0116
        logging.info("%s: Starting battery monitor." % (dt.now()).strftime("%c"))
        self._dbus_mon = DbusMon()

    def _find_settings(self):
        """Searches for settings on the DBus."""  # copilot change: C0116
        logging.info(
            "%s: Searching Settings: Trial Nr. %d"
            % ((dt.now()).strftime("%c"), (self._search_trials + 1))
        )
        try:
            for service in self._dbus_conn.list_names():
                if "com.victronenergy.settings" in service:
                    self._settings = service
                    logging.info(
                        "%s: com.victronenergy.settings found."
                        % (dt.now()).strftime("%c")
                    )
        except dbus.DBusException:  # copilot change: W0718
            logging.error(
                "%s: Error accessing DBus services." % (dt.now()).strftime("%c")
            )

        if self._settings is not None:
            self._search_trials = 0
            GLib.timeout_add(
                5000, self._find_batteries
            )
            return False
        elif self._search_trials < settings.SEARCH_TRIALS:
            self._search_trials += 1
            return True
        else:
            logging.error(
                "%s: com.victronenergy.settings not found. Exiting."
                % (dt.now()).strftime("%c")
            )
            sys.exit()

    def _find_batteries(self):
        """Searches for physical batteries on the DBus."""  # copilot change: C0116
        self._batteries_dict = {}
        batteries_count = 0  # copilot change: C0103
        product_name = ""  # copilot change: C0103
        logging.info(
            "%s: Searching batteries: Trial Nr. %d"
            % ((dt.now()).strftime("%c"), (self._search_trials + 1))
        )
        try:
            for service in self._dbus_conn.list_names():
                if "com.victronenergy" in service:
                    logging.info(
                        "%s: Dbusmonitor sees: %s"
                        % ((dt.now()).strftime("%c"), service)
                    )
                if settings.BATTERY_SERVICE_NAME in service:
                    product_name = self._dbus_mon.dbusmon.get_value(
                        service, settings.BATTERY_PRODUCT_NAME_PATH
                    )
                    if (product_name is not None) and (
                        settings.BATTERY_PRODUCT_NAME in product_name
                    ):
                        logging.info(
                            "%s: Correct battery product name %s found in the service %s"
                            % ((dt.now()).strftime("%c"), product_name, service)
                        )
                        try:
                            battery_name = self._dbus_mon.dbusmon.get_value(
                                service, settings.BATTERY_INSTANCE_NAME_PATH
                            )
                        except KeyError:
                            battery_name = "Battery%d" % (batteries_count + 1)
                        if battery_name in self._batteries_dict:
                            battery_name = "%s%d" % (battery_name, batteries_count + 1)

                        self._batteries_dict[battery_name] = service
                        logging.info(
                            "%s: %s found, named as: %s."
                            % (
                                (dt.now()).strftime("%c"),
                                (
                                    self._dbus_mon.dbusmon.get_value(
                                        service, "/ProductName"
                                    )
                                ),
                                battery_name,
                            )
                        )

                        batteries_count += 1

        except dbus.DBusException:  # copilot change: W0718
            logging.error(
                "%s: Error accessing DBus services." % (dt.now()).strftime("%c")
            )
        logging.info(
            "%s: %d batteries found." % ((dt.now()).strftime("%c"), batteries_count)
        )

        if batteries_count == settings.NR_OF_BATTERIES:
            if settings.CURRENT_FROM_VICTRON:
                self._search_trials = 0
                GLib.timeout_add(
                    1000, self._find_multis
                )
            else:
                self._time_old = tt.time()
                GLib.timeout_add(
                    1000, self._update
                )
            return False
        elif self._search_trials < settings.SEARCH_TRIALS:
            self._search_trials += 1
            return True
        else:
            logging.error(
                "%s: Required number of batteries not found. Exiting."
                % (dt.now()).strftime("%c")
            )
            sys.exit()

    def _update(self):
        """Aggregates values of physical batteries and updates the DBus."""  # copilot change: C0116
        try:
            for battery_service in self._batteries_dict.values():
                MaxCellTemp_list.append(
                    self._fn.max_value(
                        self._dbus_mon.dbusmon.get_value(
                            battery_service, "/System/MaxCellTemperature"
                        )
                    )
                )
                MinCellTemp_list.append(
                    self._fn.min_value(
                        self._dbus_mon.dbusmon.get_value(
                            battery_service, "/System/MinCellTemperature"
                        )
                    )
                )
        except Exception as err:
            logging.error("%s: Error: %s." % ((dt.now()).strftime("%c"), err))

        MaxCellTemp = self._fn.max_value(MaxCellTemp_list)
        MinCellTemp = self._fn.min_value(MinCellTemp_list)

        LowVoltage_alarm = self._fn.max_value(LowVoltage_alarm_list)
        HighVoltage_alarm = self._fn.max_value(HighVoltage_alarm_list)
        LowCellVoltage_alarm = self._fn.max_value(LowCellVoltage_alarm_list)
        LowSoc_alarm = self._fn.max_value(LowSoc_alarm_list)
        HighChargeCurrent_alarm = self._fn.max_value(HighChargeCurrent_alarm_list)
        HighDischargeCurrent_alarm = self._fn.max_value(HighDischargeCurrent_alarm_list)
        CellImbalance_alarm = self._fn.max_value(CellImbalance_alarm_list)
        InternalFailure_alarm = self._fn.max_value(InternalFailure_alarm_list)
        HighChargeTemperature_alarm = self._fn.max_value(HighChargeTemperature_alarm_list)
        LowChargeTemperature_alarm = self._fn.max_value(LowChargeTemperature_alarm_list)
        HighTemperature_alarm = self._fn.max_value(HighTemperature_alarm_list)
        LowTemperature_alarm = self._fn.max_value(LowTemperature_alarm_list)
        BmsCable_alarm = self._fn.max_value(BmsCable_alarm_list)

        MaxChargeVoltage = self._fn.min_value(MaxChargeVoltage_list)
        MaxChargeCurrent = self._fn.min_value(MaxChargeCurrent_list) * settings.NR_OF_BATTERIES
        MaxDischargeCurrent = self._fn.min_value(MaxDischargeCurrent_list) * settings.NR_OF_BATTERIES

        AllowToCharge = self._fn.min_value(AllowToCharge_list)
        AllowToDischarge = self._fn.min_value(AllowToDischarge_list)
        AllowToBalance = self._fn.min_value(AllowToBalance_list)

        MaxChargeCurrent = settings.MAX_CHARGE_CURRENT * self._fn.interpolate(
            settings.CELL_CHARGE_LIMITING_VOLTAGE,
            settings.CELL_CHARGE_LIMITED_CURRENT,
            MaxCellVoltage,
        )
        MaxDischargeCurrent = settings.MAX_DISCHARGE_CURRENT * self._fn.interpolate(
            settings.CELL_DISCHARGE_LIMITING_VOLTAGE,
            settings.CELL_DISCHARGE_LIMITED_CURRENT,
            MinCellVoltage,
        )

        if settings.SEND_CELL_VOLTAGES == 1:
            for _, current_cell in enumerate(cellVoltages_dict):
                bus[
                    "/Voltages/%s" % (re.sub("[^A-Za-z0-9_]+", "", current_cell))
                ] = cellVoltages_dict[current_cell]


def main():
    """Main function to start the AggregateBatteries service."""  # copilot change: C0116
    logging.basicConfig(level=logging.INFO)
    logging.info("%s: Starting AggregateBatteries." % (dt.now()).strftime("%c"))
    from dbus.mainloop.glib import DBusGMainLoop

    DBusGMainLoop(set_as_default=True)

    DbusAggBatService()

    logging.info(
        "%s: Connected to DBus, and switching over to GLib.MainLoop()"
        % (dt.now()).strftime("%c")
    )
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == "__main__":
    main()
