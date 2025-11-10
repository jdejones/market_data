from market_data import pd, operator


class InterestList:
    def __init__(self):
        self.interest_list = []

    def value_filter(self, sym_value: list[tuple[str, float]], threshold: float, 
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
        for sym, value in sym_value:
            if op(value, threshold):
                sym_data = symbols[sym]
                if sym_data.interest_factor is None:
                    sym_data.interest_factor = [interest_factor]
                else:
                    sym_data.interest_factor.append(interest_factor)
                sym_data.interest_direction = interest_direction
                self.interest_list.append(sym)

    def __call__(self):
        return self.interest_list
