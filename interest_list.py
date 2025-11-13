from market_data import pd, operator, warnings
from typing import Iterable


class InterestList:
    def __init__(self, source_symbols: dict[str, pd.DataFrame]):
        self.interest_list = []
        self.source_symbols = source_symbols

    def value_filter(self, sym_value: Iterable[tuple[str, float]], threshold: float, 
                     _operator: str, interest_factor: str, interest_direction: str):
        if _operator == '>':
            op = operator.gt
        elif _operator == '<':
            op = operator.lt
        elif _operator == '==':
            op = operator.eq
        elif _operator == '!=':
            op = operator.ne
        elif _operator == '>=':
            op = operator.ge
        elif _operator == '<=':
            op = operator.le
        for item in sym_value:
            if len(item) != 2:
                warnings.warn(f'Expected tuple of (symbol, value), got {item}')
                continue
            sym, value = item[0], item[1]
            if type(sym) is not str or type(value) not in [int, float]:
                warnings.warn(f'Expected tuple of (symbol: str, value: float), got {(item[0], item[1])}')
                continue
            if op(value, threshold):
                sym_data = self.source_symbols[sym]
                if sym_data.interest_factor is None:
                    sym_data.interest_factor = [interest_factor]
                else:
                    sym_data.interest_factor.append(interest_factor)
                sym_data.interest_direction = interest_direction
                self.interest_list.append(sym)

    def __call__(self):
        return self.interest_list
