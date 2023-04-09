from dash import dcc, html
import dash_bootstrap_components as dbc
from app_utilities import load_config
from pathlib import Path

CONFIG_PATH = Path("./config/config.yaml").absolute()


def layout() -> dbc.Container:
    return dbc.Container(
        # style={"width": 15600},
        children=[
            dcc.Store(id="store-config", data=load_config(path=CONFIG_PATH)),
            dcc.Store(id="store-hdf-path", data=None),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dcc.RadioItems(
                                id="radio-period",
                                options={"D": "Daily", "W": "Weekly", "M": "Monthly"},
                                value="D",
                                inline=False,
                            )
                        ],
                        width={"size": 1, "offset": 0},
                    ),
                    # dbc.Col(
                    #     [
                    #         dcc.RadioItems(
                    #             id="radio-candle",
                    #             options={"ha": "Heikin Ashi", "ohlc": "OHLC"},
                    #             value="ha",
                    #             inline=False,
                    #         )
                    #     ],
                    #     width={"size": 1, "offset": 0},
                    # ),
                    dbc.Col(
                        [
                            dcc.Checklist(
                                id="checklist-screeners",
                                options=[],
                                value=[],
                                inline=False,
                            )
                        ],
                        width={"size": 2, "offset": 0},
                    ),
                    dbc.Col(
                        [
                            dcc.RadioItems(
                                id="radio-trend",
                                options={1: "Buy", -1: "Sell"},
                                value="1",
                                inline=False,
                            )
                        ],
                        width={"size": 1, "offset": 0},
                    ),
                    dbc.Col(
                        [
                            dcc.RangeSlider(
                                id="slider-periods",
                                # min=0,
                                # max=15,
                                step=None,
                                marks={i: str(i) for i in list(range(0, 16, 1))[::-1]},
                                value=[0, 1],
                                vertical=True,
                                allowCross=False,
                                pushable=True,
                            )
                        ],
                        width={"size": 1, "offset": 0},
                    ),
                    dbc.Col(
                        [
                            dcc.RadioItems(
                                id="radio-symbols",
                                options=[],
                                value=None,
                            )
                        ],
                        width={"size": 1, "offset": 1},
                        style={"overflow-y": "scroll", "overflow-x": "hidden", "height": "350px", "width": 150},
                    ),
                    dbc.Col(
                        [
                            dcc.Textarea(
                                id="textarea-info",
                                readOnly=True,
                                value="",
                                wrap="yes",
                                style={"width": 300, "height": 350},
                            )
                        ],
                        width={"size": 2, "offset": 0},
                    ),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col([html.Button("Refresh Plot", id="button-refresh", n_clicks=0)]),
                    dbc.Col(
                        [
                            html.H5("Candles"),
                            dcc.RadioItems(
                                id="radio-candle",
                                options={"ha": "Heikin Ashi", "ohlc": "OHLC"},
                                value="ha",
                                inline=False,
                            ),
                            html.Hr(),
                            html.H5("Indicators"),
                            dcc.Checklist(
                                id="checklist-indicators",
                                options=[],
                                value=[],
                                inline=False,
                            ),
                            html.Hr(),
                            html.H5("Lines"),
                            dcc.Checklist(
                                id="checklist-lines",
                                options=[],
                                value=[],
                                inline=False,
                            ),
                            html.Hr(),
                            html.H5("Patterns"),
                            dcc.Checklist(
                                id="checklist-patterns",
                                options=["Patterns"],
                                value=[],
                                inline=False,
                            ),
                        ],
                        width={"size": 2, "offset": 0},
                    ),
                    dbc.Col([dcc.Graph(id="graph-candlestick")], width={"size": 8, "offset": 0}),
                ]
            ),
        ],
    )
