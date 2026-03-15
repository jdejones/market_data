from market_data import pd, operator, warnings, Iterable


class InterestList:
    """
    Collect symbols that satisfy value-based interest filters.

    This class maintains an internal list of symbols that pass numeric
    comparisons and annotates the corresponding entries in `source_symbols`
    with interest metadata. Matching symbols are appended in insertion order,
    and the mapped symbol objects are mutated in place.
    """
    def __init__(self, source_symbols: dict[str, pd.DataFrame]):
        """
        Initialize an interest-list accumulator for a symbol mapping.

        The constructor stores the symbol mapping used later by `value_filter`
        and initializes an empty list for matches. `value_filter` looks up each
        passing symbol in `source_symbols` and mutates the retrieved object
        through its `interest_factor`, `interest_direction`, and
        `interest_source` attributes.

        Parameters
        ----------
        source_symbols : dict[str, pd.DataFrame]
            Mapping keyed by symbol string. `value_filter` indexes this mapping
            with each passing symbol and mutates the retrieved object by setting
            `interest_direction`, appending to `interest_source`, and either
            initializing or appending to `interest_factor`.
        """
        self.interest_list = []
        self.source_symbols = source_symbols

    def value_filter(self, sym_value: Iterable[tuple[str, float]], threshold: float, 
                     _operator: str, interest_factor: str, interest_direction: str,
                     interest_source: str):
        """
        Append symbols whose values satisfy a comparison against a threshold.

        The method iterates through `sym_value`, checks that each item can be
        treated as a two-element `(symbol, value)` pair, applies the comparison
        selected by `_operator`, and records every passing symbol. For each
        match, the method mutates the corresponding object in `source_symbols`
        by appending `interest_factor`, overwriting `interest_direction`, and
        appending `interest_source`.

        Parameters
        ----------
        sym_value : Iterable[tuple[str, float]]
            Iterable producing candidate `(symbol, value)` pairs. The
            implementation accepts any iterable, but each yielded item must have
            length 2 so it can be indexed as `item[0]` and `item[1]`. The first
            element must be a symbol string present in `source_symbols`, and the
            second element must be an `int` or `float`.
        threshold : float
            Numeric threshold compared against each `value` using `_operator`.
            The comparison is evaluated as `op(value, threshold)`.
        _operator : str
            Comparison operator string used to choose the binary predicate.
            Supported literal values are `'>'`, `'<'`, `'=='`, `'!='`, `'>='`,
            and `'<='`.
        interest_factor : str
            Label appended to the matched symbol object's `interest_factor`
            list. If `interest_factor` is currently `None`, the method replaces
            it with a new one-element list containing this value.
        interest_direction : str
            Direction label assigned directly to the matched symbol object's
            `interest_direction` attribute. Existing values are overwritten.
        interest_source : str
            Source label appended to the matched symbol object's
            `interest_source` list.

        Notes
        -----
        -Items that are not length-2 pairs or do not contain `(str, int|float)`
        trigger `warnings.warn(...)` and are skipped. 
        -The method does not deduplicate symbols: repeated matches append repeated entries to both
        `self.interest_list` and the per-symbol metadata lists. 
        -The method also does not copy `source_symbols` or the retrieved symbol objects.
        """
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
                sym_data.interest_source.append(interest_source)
                self.interest_list.append(sym)

    def __call__(self):
        """
        Return the accumulated list of matching symbols.

        Calling the instance returns the same list object stored in
        `self.interest_list`. The list reflects all prior successful
        `value_filter` calls, preserves insertion order, and may contain
        duplicate symbols.

        Returns
        -------
        list[str]
            Internal list of symbol strings appended by `value_filter`.
        """
        return self.interest_list
