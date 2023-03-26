from plotly.subplots import make_subplots
import plotly.graph_objects as go
from pandas.tseries.holiday import USFederalHolidayCalendar
import datetime
from pandas.tseries.offsets import CustomBusinessDay
from pandas import DataFrame
from numpy import where
from plotly.graph_objects import Figure


USFEDHOLIDAYS = USFederalHolidayCalendar()
# USFEDHOLIDAYS.merge(GoodFriday, inplace=True)
MARKET_HOLIDAYS = [
    i.astype(datetime.datetime).strftime("%Y-%m-%d") for i in list(CustomBusinessDay(calendar=USFEDHOLIDAYS).holidays)
][200:700]
RANGEBREAKS = [dict(bounds=["sat", "mon"]), dict(values=MARKET_HOLIDAYS)]  # ,  # hide weekends


def clear_figure() -> Figure:
    return []


def create_figure(df: DataFrame, symbol: str, indicators: dict) -> Figure:
    """Plot the data on the main graph"""
    if indicators is not None:
        num_indicators = len(indicators)
    else:
        num_indicators = 0
        indicators = {}

    row_heights = [1] + num_indicators * [0.025] + [0.2]
    num_rows = 2 + num_indicators
    i_rows = range(1, num_rows + 1)
    print("i_rows", i_rows)
    fig = make_subplots(rows=num_rows, cols=1, row_heights=row_heights, shared_xaxes=True, vertical_spacing=0.005)

    fig.update_xaxes(rangebreaks=RANGEBREAKS)

    if df is None:
        return fig

    if df.empty:
        return fig
    
    fig.add_trace(
        row=1,
        col=1,
        trace=go.Candlestick(
            name="candlestick",
            showlegend=False,
            x=[] if len(df) == 0 else df.Date,  # .Date.dt.strftime("%d/%m/%y %-H:%M"),
            open=df.Open,
            high=df.High,
            low=df.Low,
            close=df.Close,
        ),
    )
    volume_color = df.Close.astype("float") - df.Open.astype("float")
    volume_colors = {-1: "#FF0000", 0: "#C0C0C0", 1: "#009900"}
    volume_color = list(
        where(
            volume_color > 0, volume_colors.get(1), where(volume_color < 0, volume_colors.get(-1), volume_colors.get(0))
        )
    )
    print("indicators", indicators)
    # y_labels = []
    for i, (k, v) in enumerate(indicators.items()):
        print(i)
        # print(k)
        # y_labels.append(k)
        print(v)
        colorscale = {-1: "red", 0: "lightgrey", 1: "green"}  # [[-1.0, "red"], [0.0, "lightgrey"], [1.0, "green"]]
        trace = go.Bar(
            name=k,
            y=[1] * len(v),  # [["Hiekin Ashi"]],
            x=v.Date.tolist(),
            marker={"color": [colorscale[i] for i in v.value.tolist()]},
            showlegend=False,
        )
        fig.add_trace(
            trace=trace,
            row=[i_rows[i] + 1],
            col=1,
        )
        # fig.add_annotation(x=0, y=0, text=f"{k}")

    fig.add_trace(
        row=i_rows[-1],
        col=1,
        trace=go.Bar(name="Volume", showlegend=False, x=df.Date, marker_color=volume_color, y=df.Volume),
    )

    fig.update(layout_xaxis_rangeslider_visible=False)

    fig.update_layout(get_layout_params(symbol=symbol, y_axes=i_rows))

    return fig


def get_layout_params(symbol: str, y_axes: list[int]):
    """Create the layout parameters"""
    symbol = symbol if symbol is not None else " "
    layout = {
        "width": 1400,
        "height": 800,
        "title": symbol,
        "xaxis1": {
            "rangeselector": {
                "buttons": [
                    {"count": 5, "label": "5d", "step": "day", "stepmode": "backward"},
                    {"count": 15, "label": "15d", "step": "day", "stepmode": "backward"},
                    {"count": 1, "label": "1m", "step": "month", "stepmode": "backward"},
                    {"count": 3, "label": "3m", "step": "month", "stepmode": "backward"},
                    {"count": 6, "label": "6m", "step": "month", "stepmode": "backward"},
                    # {'count': 1, 'label': "YTD", 'step': "year", 'stepmode': "todate"},
                    {"count": 1, "label": "1y", "step": "year", "stepmode": "backward"},
                    {"count": 2, "label": "2y", "step": "year", "stepmode": "backward"},
                    {"count": 5, "label": "5y", "step": "year", "stepmode": "backward"},
                    {"step": "all"},
                ]
            },
            "type": "date",
            "rangeslider": {"visible": False},
        },
        "yaxis1": {"title": {"text": "Price $ - US Dollars"}},
        f"yaxis{y_axes[-1]}": {"title": {"text": "Volume"}},
    }

    for i, val in enumerate(y_axes[1:-1]):
        layout[f"yaxis{val}"] = {
            "visible": True,
            "showticklabels": False,
            # "title": {"text": y_labels[i]}
        }

    return layout
