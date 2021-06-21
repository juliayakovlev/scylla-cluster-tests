# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

from sdcm.sct_events.base import ContinuousEvent


class DisruptionEvent(ContinuousEvent):
    def __init__(self,
                 nemesis_name,
                 # subtype,
                 severity,
                 node,
                 publish_event=True,
                 # error=None,
                 # full_traceback=None,
                 **kwargs):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__(severity=severity, publish_event=publish_event)

        self.nemesis_name = nemesis_name
        # self.subtype = subtype
        self.node = str(node)
        # self.start = start
        # self.end = end
        self.duration = None

        # self.error = None
        self.full_traceback = ""
        # self.skip_reason = ""
        # if error:
        #     self.error = error
        #     self.full_traceback = str(full_traceback)
        self.kwargs = kwargs
        self.__dict__.update(self.kwargs)

    @property
    def msgfmt(self) -> str:
        fmt = super().msgfmt + ": nemesis_name={0.nemesis_name} target_node={0.node}"
        if self.skip_reason:
            fmt += " skip_reason={0.skip_reason}"
        if self.errors:
            fmt += " errors={0.errors}"
        if self.full_traceback:
            fmt += "\n{0.full_traceback}"
        # if self.severity == Severity.ERROR:
        #     fmt += " error={0.error}\n{0.full_traceback}"
        return fmt
