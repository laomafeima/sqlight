from typing import List


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    _row_original = None

    def get_changed(self) -> List[str]:
        result = []
        if self._row_original is None:
            return result

        for k, v in self._row_original.items():
            if v != self[k]:
                result.append(k)

        return result

    def _before_change_value(self, name, value):
        if self[name] == value:
            return
        if self._row_original is None:
            super().__setattr__("_row_original", {})
        if name in self._row_original.keys():
            return
        self._row_original[name] = self[name]

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self.__setitem__(name, value)

    def __setitem__(self, name, value):
        if name not in self.keys():
            raise AttributeError(name)
        self._before_change_value(name, value)
        super().__setitem__(name, value)
