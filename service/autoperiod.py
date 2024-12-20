from __future__ import annotations

import sys
import warnings
from typing import Any, List, Tuple, Optional, Union

import numpy as np
import statsmodels.tsa.stattools
from scipy.optimize import minimize_scalar
from scipy.signal import periodogram
from scipy.stats import linregress


def pearson_corr(a, b):
    return np.dot((a - np.mean(a)), (b - np.mean(b))) / ((np.linalg.norm(a - np.mean(a))) * (np.linalg.norm(b - np.mean(b))))


def calc_seasonality_score(data):
    period = autoperiod(data)
    print(period)
    if period <= 1:
        return 0, 0
    
    tmp = period
    corr_sum = 0
    cnt = 0
    while tmp < 157-period:
        corr_sum += pearson_corr(data[tmp:], data[:-tmp])
        print(cnt+1, pearson_corr(data[tmp:], data[:-tmp]))
        tmp += period
        cnt += 1

    print(period, corr_sum/cnt)

    return period, corr_sum / cnt


# from https://github.com/CodeLionX/periodicity-detection
def autoperiod(
    data: np.ndarray,
    *,
    pt_n_iter: int = 100,
    random_state: Any = None,
    detrend: bool = False,
    use_number_peaks_fallback: bool = False,
    number_peaks_n: int = 100,
    acf_hill_steepness: float = 0.0,
) -> int:
    """AUTOPERIOD method calculates the period in a two-step process. First, it
    extracts candidate periods from the periodogram (using an automatically
    determined power threshold, see ``pt_n_iter`` parameter). Then, it uses the circular
    autocorrelation to validate the candidate periods. Periods on a hill of the ACF
    with sufficient steepness are considered valid. The candidate period with the
    highest power is returned.

    Changes compared to the paper:

    - Potential detrending of the time series before estimating the period.
    - Potentially returns multiple detected periodicities.
    - Option to use the number of peaks method as a fallback if no periods are found.
    - Potentially exclude periods, whose ACF hill is not steep enough.

    Parameters
    ----------
    data : np.ndarray
        Array containing the data of a univariate, equidistant time series.
    pt_n_iter : int
        Number of shuffling iterations to determine the power threshold. The higher the
        number, the tighter the confidence interval. The percentile is calculated using
        :math:`percentile = 1 - 1 / pt\\_n\\_iter`.
    random_state : Any
        Seed for the random number generator. Used for determining the power threshold
        (data shuffling).
    detrend : bool
        Removes linear trend from the time series before calculating the candidate
        periods. (Addition to original method).
    use_number_peaks_fallback : bool
        If ``True`` and no periods are found, the number of peaks method is used as a
        fallback. (Addition to original method).
    number_peaks_n: int
        Number of peaks to return when using the number of peaks method as a fallback.
    acf_hill_steepness : float
        Minimum steepness of the ACF hill to consider a period valid. The higher the
        value, the steeper the hill must be. A value of ``0`` means that any hill is
        considered valid. The threshold is applied to the sum of the absolute slopes of
        the two fitted lines left and right of the candidate period.

    Examples
    --------

    Estimate the period length of a simple sine curve:

    >>> import numpy as np
    >>> rng = np.random.default_rng(42)
    >>> data = np.sin(np.linspace(0, 8*np.pi, 1000)) + rng.random(1000)/10
    >>> from periodicity_detection import autoperiod
    >>> period = autoperiod(data, random_state=42, detrend=True)

    See Also
    --------
    `<https://epubs.siam.org/doi/epdf/10.1137/1.9781611972757.40>`_ : Paper reference
    """
    return Autoperiod(  # type: ignore
        pt_n_iter=pt_n_iter,
        random_state=random_state,
        detrend=detrend,
        use_number_peaks_fallback=use_number_peaks_fallback,
        number_peaks_n=number_peaks_n,
        acf_hill_steepness=acf_hill_steepness,
        plot=False,
        verbose=0,
        return_multi=1,
    )(data)


class Autoperiod:
    """AUTOPERIOD method to calculate the most dominant periods in a time series using
    the periodogram and the autocorrelation function (ACF).

    For more details, please see :func:`periodicity_detection.autoperiod`!

    Parameters
    ----------
    pt_n_iter : int
        Number of shuffling iterations to determine the power threshold. The higher the
        number, the tighter the confidence interval. The percentile is calculated using
        :math:`percentile = 1 - 1 / pt_n_iter`.
    random_state : Any
        Seed for the random number generator. Used for determining the power threshold
        (data shuffling).
    plot : bool
        Show the periodogram and ACF plots.
    verbose : int
        Controls the log output verbosity. If set to ``0``, no messages are printed;
        when ``>=3``, all messages are printed.
    detrend : bool
        Removes linear trend from the time series before calculating the candidate
        periods. (Addition to original method).
    use_number_peaks_fallback : bool
        If ``True`` and no periods are found, the number of peaks method is used as a
        fallback. (Addition to original method).
    number_peaks_n: int
        Number of peaks to return when using the number of peaks method as a fallback.
    return_multi : int
        Maximum number of periods to return.
    acf_hill_steepness : float
        Minimum steepness of the ACF hill to consider a period valid. The higher the
        value, the steeper the hill must be. A value of ``0`` means that any hill is
        considered valid. The threshold is applied to the sum of the absolute slopes of
        the two fitted lines left and right of the candidate period.

    Examples
    --------
    Plot the periodogram and ACF while computing the period size:

    >>> import numpy as np
    >>> import matplotlib.pyplot as plt
    >>> rng = np.random.default_rng(42)
    >>> data = np.sin(np.linspace(0, 8*np.pi, 1000)) + rng.random(1000)/10
    >>> period = Autoperiod(random_state=42, detrend=True, plot=True)(data)
    >>> plt.show()
    """

    # potential improvement:
    # https://link.springer.com/chapter/10.1007/978-3-030-39098-3_4
    def __init__(
        self,
        *,
        pt_n_iter: int = 100,
        random_state: Any = None,
        plot: bool = False,
        verbose: int = 0,
        detrend: bool = False,
        use_number_peaks_fallback: bool = False,
        number_peaks_n: int = 100,
        return_multi: int = 1,
        acf_hill_steepness: float = 0.0,
    ):
        self._pt_n_iter = pt_n_iter
        self._rng: np.random.Generator = np.random.default_rng(random_state)
        self._plot = plot
        self._verbosity = verbose
        self._detrend = detrend
        self._use_np_fb = use_number_peaks_fallback
        self._np_n = number_peaks_n
        self._trend: Optional[np.ndarray] = None
        self._orig_data: Optional[np.ndarray] = None
        self._return_multi = return_multi
        self._acf_hill_steepness = acf_hill_steepness

    def __call__(self, data: np.ndarray) -> Union[List[int], int]:
        """Estimate the period length of a time series.

        Parameters
        ----------
        data : np.ndarray
            Array containing the data of a univariate, equidistant time series.

        Returns
        -------
        periods : Union[List[int], int]
            List of periods sorted by their power. If ``return_multi`` is set to ``1``,
            only the most dominant period is returned.
        """
        if self._detrend:
            self._print("Detrending")
            index = np.arange(data.shape[0])
            trend_fit = linregress(index, data)
            if trend_fit.slope > 1e-4:
                trend = trend_fit.intercept + index * trend_fit.slope

                data = data - trend
                self._print(
                    f"removed trend with slope {trend_fit.slope:.6f} "
                    f"and intercept {trend_fit.intercept:.4f}",
                    level=2,
                )
            else:
                self._print(
                    f"skipping detrending because slope ({trend_fit.slope:.6f}) "
                    f"is too shallow (< 1e-4)",
                    level=2,
                )
                self._print(f"removing remaining mean ({data.mean():.4f})", level=2)
                data = data - data.mean()

        if self._verbosity > 1:
            self._print("Determining power threshold")
        p_threshold = self._power_threshold(data)
        self._print(f"Power threshold: {p_threshold:.6f}")

        if self._verbosity > 1:
            self._print("\nDiscovering candidate periods (hints) from periodogram")
        period_hints = self._candidate_periods(data, p_threshold)
        self._print(f"{len(period_hints)} candidate periods (hints)")

        if self._verbosity > 1:
            self._print("\nVerifying hints using ACF")
        periods = self._verify(data, period_hints)

        if len(periods) < 1 or periods[0] <= 1 and self._use_np_fb:
            self._print(
                f"\nDetected invalid period ({periods}), "
                f"falling back to number_peaks method"
            )
            periods = [number_peaks(data, n=self._np_n)]
        self._print(f"Periods are {periods}")
        if self._return_multi > 1:
            return periods[: self._return_multi]
        else:
            return int(periods[0])

    def _print(self, msg: str, level: int = 1) -> None:
        if self._verbosity >= level:
            print("  " * (level - 1) + msg, file=sys.stderr)

    def _power_threshold(self, data: np.ndarray) -> float:
        n_iter = self._pt_n_iter
        percentile = 1 - 1 / n_iter
        self._print(
            f"determined confidence interval as {percentile} "
            f"(using {n_iter} iterations)",
            level=2,
        )
        max_powers = []
        values = data.copy()
        for i in range(n_iter):
            self._rng.shuffle(values)
            _, p_den = periodogram(values)
            max_powers.append(np.max(p_den))
        max_powers.sort()
        return max_powers[-1]

    def _candidate_periods(
        self, data: np.ndarray, p_threshold: float
    ) -> List[Tuple[int, float, float]]:
        N = data.shape[0]
        f, p_den = periodogram(data)
        # k are the DFT bin indices (see paper)
        k = np.array(f * N, dtype=np.int_)
        # print("k:", k)
        # print("frequency:", f)  # between 0 and 0.5
        # print("period:", N/k)

        self._print(
            f"inspecting periodogram between 2 and {N // 2} (frequencies 0 and 0.5)",
            level=2,
        )
        period_hints = []
        for i in np.arange(2, N // 2):
            if p_den[i] > p_threshold:
                period_hints.append((k[i], f[i], p_den[i]))
                self._print(
                    f"detected hint at bin k={k[i]} (f={f[i]:.4f}, "
                    f"power={p_den[i]:.2f})",
                    level=3,
                )

        # start with the highest power frequency:
        self._print("sorting hints by highest power first", level=2)
        period_hints = sorted(period_hints, key=lambda x: x[-1], reverse=True)

        return period_hints

    def _verify(
        self, data: np.ndarray, period_hints: List[Tuple[int, float, float]]
    ) -> List[int]:
        # produces wrong acf:
        # acf = fftconvolve(data, data[::-1], 'full')[data.shape[0]:]
        # acf = acf / np.max(acf)
        acf = statsmodels.tsa.stattools.acf(data, fft=True, nlags=data.shape[0])
        index = np.arange(acf.shape[0])
        N = data.shape[0]
        ranges = []

        warnings.filterwarnings(
            action="ignore",
            category=RuntimeWarning,
            message=r".*invalid value encountered.*",
        )
        for k, f, power in period_hints:
            if k < 2:
                self._print(f"processing hint at {N // k}: k={k}, f={f}", level=2)
                self._print("k < 2 --> INVALID", level=3)
                continue

            # determine search interval
            begin = int((N / (k + 1) + N / k) / 2) - 1
            end = int((N / k + N / (k - 1)) / 2) + 1
            while end - begin < 4:
                if begin > 0:
                    begin -= 1
                if end < N - 1:
                    end += 1
            self._print(
                f"processing hint at {N // k}, k={k}: begin={begin}, end={end + 1}",
                level=2,
            )
            slopes = {}

            def two_segment(t: float, args: List[np.ndarray]) -> float:
                x, y = args
                t = int(np.round(t))
                slope1 = linregress(x[:t], y[:t])
                slope2 = linregress(x[t:], y[t:])
                slopes[t] = (slope1, slope2)
                error = np.sum(
                    np.abs(y[:t] - (slope1.intercept + slope1.slope * x[:t]))
                ) + np.sum(np.abs(y[t:] - (slope2.intercept + slope2.slope * x[t:])))
                return error

            # print("outer indices", begin, end+1)
            # print("inner indices", 0, end - begin + 1)
            # print("bounds", 2, end - begin - 2)
            res = minimize_scalar(
                two_segment,
                args=[index[begin : end + 1], acf[begin : end + 1]],
                method="bounded",
                bounds=(2, end - begin - 2),
                options={
                    "disp": 1 if self._verbosity > 2 else 0,
                    "xatol": 1e-8,
                    "maxiter": 500,
                },
            )
            if not res.success:
                # self._print(f"curve fitting failed ({res.message}) --> INVALID", l=3)
                # continue
                raise ValueError(
                    "Failed to find optimal midway-point for slope-fitting "
                    f"(hint: k={k}, f={f}, power={power})!"
                )

            t = int(np.round(res.x))
            optimal_t = begin + t
            slope = slopes[t]
            self._print(f"found optimal t: {optimal_t} (t={t})", level=3)

            # change from paper: we require a certain hill size to prevent noise
            # influencing our results:
            lslope = slope[0].slope
            rslope = slope[1].slope
            steepness = np.abs(lslope) + np.abs(rslope)
            if lslope < 0 < rslope:
                self._print("valley detected --> INVALID", level=3)

            elif steepness < self._acf_hill_steepness:
                self._print(
                    f"insufficient steepness ({np.abs(slope[0].slope):.4f} and "
                    f"{np.abs(slope[1].slope):.4f}) --> INVALID",
                    level=3,
                )

            elif lslope > 0 > rslope:
                self._print("hill detected --> VALID", level=3)
                period = begin + np.argmax(acf[begin : end + 1])
                self._print(f"corrected period (from {N // k}): {period}", level=3)
                ranges.append((begin, end, optimal_t, period, slope))
                if self._return_multi <= 1:
                    break

            else:
                self._print("not a hill, but also not a valley --> INVALID", level=3)

        warnings.filterwarnings(
            action="default",
            category=RuntimeWarning,
            message=r".*invalid value encountered.*",
        )

        periods = list(x[3] for x in ranges)
        if len(periods) > 0:
            return np.unique(periods)[::-1][: self._return_multi].astype(int)
        else:
            return [1]


def number_peaks(data: np.ndarray, n: int) -> int:
    """Determines the period size based on the number of peaks. This method is based on
    tsfresh's implementation of the same name:
    :func:`~tsfresh.feature_extraction.feature_calculators.number_peaks`.

    Calculates the number of peaks of at least support :math:`n` in the time series.
    A peak of support :math:`n` is defined as a subsequence where a value occurs, which
    is bigger than its :math:`n` neighbours to the left and to the right. The time
    series length divided by the number of peaks defines the period size.

    Parameters
    ----------
    data : array_like
        Time series to calculate the number of peaks of.
    n : int
        The required support for the peaks.

    Returns
    -------
    period_size : float
        The estimated period size.

    Examples
    --------

    Estimate the period length of a simple sine curve:

    >>> import numpy as np
    >>> rng = np.random.default_rng(42)
    >>> data = np.sin(np.linspace(0, 8*np.pi, 1000)) + rng.random(1000)/10
    >>> from periodicity_detection import number_peaks
    >>> period = number_peaks(data)

    See Also
    --------
    tsfresh.feature_extraction.number_peaks :
        tsfresh's implementation, on which this method is based on.
    """
    x_reduced = data[n:-n]

    res: Optional[np.ndarray] = None
    for i in range(1, n + 1):
        result_first = x_reduced > _roll(data, i)[n:-n]

        if res is None:
            res = result_first
        else:
            res &= result_first

        res &= x_reduced > _roll(data, -i)[n:-n]
    n_peaks = np.sum(res)  # type: ignore
    if n_peaks < 1:
        return 1
    return data.shape[0] // n_peaks


def _roll(a: np.ndarray, shift: int) -> np.ndarray:
    """Exact copy of tsfresh's ``_roll``-implementation:
    https://github.com/blue-yonder/tsfresh/blob/611e04fb6f7b24f745b4421bbfb7e986b1ec0ba1/tsfresh/feature_extraction/feature_calculators.py#L49  # noqa: E501

    This roll is for 1D arrays and significantly faster than ``np.roll()``.

    Parameters
    ----------
    a : array_like
        input array
    shift : int
        the number of places by which elements are shifted

    Returns
    -------
    array : array_like
        shifted array with the same shape as the input array ``a``

    See Also
    --------
    https://github.com/blue-yonder/tsfresh/blob/611e04fb6f7b24f745b4421bbfb7e986b1ec0ba1/tsfresh/feature_extraction/feature_calculators.py#L49 :  # noqa: E501
        Implementation in tsfresh.
    """
    if not isinstance(a, np.ndarray):
        a = np.asarray(a)
    idx = shift % len(a)
    return np.concatenate([a[-idx:], a[:-idx]])