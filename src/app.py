from dash import Dash, dcc, html, Input, Output, State
import dash_bootstrap_components as dbc

# import plotly.express as px
import time
from app_utilities import load_config, get_candle_data, load_screener_data, get_symbol_info
from app_graphing import create_figure
from screener import Screener

# from utilities import back_in_time
from indicators import make_indicators

# import yaml
from pandas import read_hdf  # , DataFrame

# from tqdm import tqdm
# from numpy import concatenate
from pathlib import Path
from plotly.graph_objects import Figure

app = Dash(name="Screener", suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

CONFIG_PATH = Path("./config/config.yaml").absolute()

app.layout = dbc.Container(
    [
        dcc.Store(id="store-config", data=load_config(path=CONFIG_PATH)),
        dcc.Store(id="store-hdf-path", data=None),
        # dcc.Store(id="store-screener", data=None),
        # dcc.Store(id="store-daily", data=None),
        # dcc.Store(id="store-weekly", data=None),
        # dcc.Store(id="store-monthly", data=None),
        # dbc.Row([dbc.Col(html.H4("Plotting"), width={"size": 6, "offset": 0})]),
        dbc.Row(
            [
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
                dbc.Col(
                    [dcc.Dropdown(id="dropdown-symbols", options=[], searchable=True, value=None)],
                    width={"size": 1, "offset": 1},
                ),
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
                dbc.Col(
                    [
                        dcc.RadioItems(
                            id="radio-candle",
                            options={"ha": "Heikin Ashi", "ohlc": "OHLC"},
                            value="ha",
                            inline=False,
                        )
                    ],
                    width={"size": 1, "offset": 0},
                ),
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
                        dcc.Slider(
                            id="slider-periods",
                            min=0,
                            max=15,
                            step=1,
                            value=1,
                        )
                    ],
                    width={"size": 2, "offset": 0},
                ),
                # dbc.Col([dcc.Loading(id="data-spinner", type="default", children=html.Div(id="data-is-loading"))]),
            ]
        ),
        html.Hr(),
        # dbc.Row(
        #     [
        #         dbc.Col(
        #             [
        #                 dcc.Textarea(
        #                     id="textarea-info",
        #                     readOnly=True,
        #                     value="",
        #                     wrap="yes",
        #                     style={"width": 300, "height": 100},
        #                 )
        #             ],
        #             width={"size": 2, "offset": 0},
        #         )
        #     ]
        # ),
        dbc.Row(
            [
                dbc.Col(
                    [
                        dcc.Checklist(
                            id="checklist-indicators",
                            options=[],
                            value=[],
                            inline=False,
                        )
                    ],
                    width={"size": 2, "offset": 0},
                ),
                dbc.Col([dcc.Graph(id="graph-candlestick")]),
            ]
        ),
    ]
)


# @app.callback(Output("config", "data"), Input("open-data", "value"))


@app.callback(
    Output("store-hdf-path", "data"),
    Input("store-config", "data"),
)
def load_hdf_path(config: dict) -> None:
    if not config:
        return ""
    return Path(config.get("config").get("paths").get("hdf_path")).absolute().__str__()


# @app.callback([Output("store-screener", "data")], Input("store-hdf-path", "data"), State("radio-period", "value"))
# def load_screener_data(hdf_path: Path, period: str) -> list[dict]:
#     if hdf_path is None:
#         return []
#     screener = Screener(path=hdf_path, merge_data=False)

#     if period == "D":
#         # cutoff_date = back_in_time(days=20)
#         # print("cutoff_date", cutoff_date)
#         return [screener.merge_signals(period="D").to_dict(orient="records")]
#     if period == "W":
#         # cutoff_date = back_in_time(weeks=20)
#         return [screener.merge_signals(period="W").to_dict(orient="records")]
#     if period == "M":
#         # cutoff_date = back_in_time(days=20 * 30)
#         return [screener.merge_signals(period="M").to_dict(orient="records")]
#     return []


@app.callback([Output("checklist-screeners", "options")], Input("store-config", "data"))
def load_screeners(config: dict) -> list[dict]:
    if not config:
        return [None]
    screeners = config.get("config").get("screeners")
    if not screeners:
        return [None]
    return [screeners]


@app.callback([Output("checklist-indicators", "options")], Input("store-config", "data"))
def load_indicators(config: dict) -> list[dict]:
    if not config:
        return [None]
    indicators = config.get("config").get("indicators")
    if not indicators:
        return [None]
    return [indicators]


@app.callback([Output("checklist-indicators", "value")], Input("store-config", "data"))
def load_default_indicators(config: dict) -> list[dict]:
    if not config:
        return [None]
    indicators = config.get("config").get("indicators")
    if not indicators:
        return [None]
    return [list(indicators.keys())]


@app.callback(
    [Output("textarea-info", "value")],
    [Input("dropdown-symbols", "value")],
    [
        State("store-config", "data"),
        State("store-hdf-path", "data"),
    ],
)
def load_info(symbol: str, config: dict, path: Path) -> dict:
    if not symbol or not config or not path:
        return [""]
    return [
        get_symbol_info(path=path, symbol=symbol, table_key=config.get("config").get("hdf_keys").get("symbol_info"))
    ]


@app.callback(
    [Output("dropdown-symbols", "options")],
    [
        Input("store-hdf-path", "data"),
        State("store-config", "data"),
        Input("radio-period", "value"),
        Input("checklist-screeners", "value"),
        Input("slider-periods", "value"),
        # State("store-screener", "data"),
    ],
)
def load_symbols(
    path: Path,
    config: dict,
    period: str,
    screeners: list[str],
    screener_lookback: int,  # df: DataFrame
) -> list[list]:
    """Load the symbols"""
    if path is None or config is None or period is None or screeners is None or screener_lookback is None:
        return [[]]

    print(path)
    print(config)

    df = read_hdf(path, key=config.get("config").get("hdf_keys").get("screener_symbols"), mode="r").sort_values(
        "symbol", ascending=True
    )
    if len(screeners) == 0:
        return [df.symbol.drop_duplicates().to_list()]

    df_signals = load_screener_data(path=path, period=period, lookback=screener_lookback)

    if df_signals is None:
        return [df.symbol.drop_duplicates().to_list()]

    # screener =
    df_screener_symbols = (
        Screener()
        .apply_screeners(screeners=screeners, num_periods=screener_lookback, df=df_signals)
        .loc[:, ["symbol", "Date"]]
    )

    if not df.empty and not df_screener_symbols.empty and df_screener_symbols is not None and df is not None:
        print(df.columns)
        print(df_screener_symbols.columns)
        df = df.merge(df_screener_symbols, how="inner", on=["symbol"])

    return [df.symbol.sort_values().to_list()]


@app.callback(
    [Output("graph-candlestick", "figure")],
    [
        Input("dropdown-symbols", "value"),
        Input("radio-period", "value"),
        Input("radio-candle", "value"),
        Input("checklist-indicators", "value"),
    ],
    [State("store-hdf-path", "data"), State("store-config", "data")],
)
def load_plot(symbol: str, period: str, candle: str, indicators: list, path: Path, config: dict) -> list[Figure]:

    df = get_candle_data(path=path, symbol=symbol, period=period, candle=candle)
    # indicators = ["ha", "st", "macd", "srsi"]
    indicators = make_indicators(path=path, period=period, symbol=symbol, indicators=indicators)
    fig = create_figure(df=df, symbol=symbol, indicators=indicators)

    return [fig]

    # if not path:
    #     return []
    # print(path)
    # print(config)
    # return [
    #     (
    #         read_hdf(path, key=config.get("config").get("hdf_keys").get("symbols"), mode="r")
    #         .symbol.sort_values()
    #         .to_list()
    #     )
    # ]


# dcc.Dropdown(id="Files", options=[], value=None),
# dcc.RadioItems(
#     id="division-selector",
#     options=[],  # ["MLB", "AL", "NL"] + ["AL East", "AL Central", "AL West", "NL East", "NL Central", "NL West"],
#     value=None,
#     labelStyle={"display": "inline-block"},
# ),
# dcc.Graph(id="graph"),
# dcc.Store(id="teams", data=None),
# dcc.Store(id="winloss", data=None),


# @app.callback(
#     Output("graph", "figure"),
#     [
#         State("teams", "data"),
#         Input("winloss", "data"),
#         State("division-selector", "value"),
#     ],
# )
# def update_winloss_data(
#     teams: list[dict],
#     winloss: list[dict],
#     division: str,
# ) -> px.line:
#     print("updating winloss on plot")
#     teams_df = DataFrame(teams)
#     winloss_df = DataFrame(winloss)

#     print(teams_df)
#     print(winloss_df)
#     if teams_df.empty:
#         return px.line(data_frame=None)

#     teams_df = teams_df.loc[teams_df.key == division]
#     return make_plot(team_records=teams_df)


# @app.callback(Output("winloss", "data"), Input("teams", "data"))
# def get_winloss(teams: list[dict]) -> list[dict]:
#     print("get winloss values")
#     # team_records = [
#     #     get_team_winloss(team_url=team["url"], team_name=team["team"])
#     #     for team in tqdm(teams, desc="Getting team records")
#     # ]

#     team_records = team_winloss_parallel(teams=teams)

#     # print(team_records[0])
#     return team_records
#     # return concat(team_records).reset_index(drop=True).to_dict(orient="records")


# def make_plot(team_records: list[DataFrame]) -> px.line:
#     """Makes the plot"""
#     print("making plot")
#     if not team_records.empty:
#         team_records = concat(team_records).reset_index(drop=True)
#         return px.line(team_records, x="game_number", y="winpct", color="team_name")
#     return px.line(data_frame=None)


# @app.callback(Output("division-selector", "options"), Input("teams", "data"))
# def update_divisions(teams: list[dict]) -> list:
#     # leagues = ["AL", "NL"]
#     # divisions = concatenate(
#     #     [[f"{league} {division}" for division in DataFrame(teams).division.unique()] for league in leagues]
#     # )

#     # print("updating divisions")
#     # # print(teams)
#     # # return concatenate([["MLB"], leagues, divisions])
#     return format_divisions(teams)


# @app.callback(Output("teams", "data"), Input("season", "value"))
# def get_teams(season: int) -> list[dict]:
#     if not season:
#         print("no season selected")
#         return []
#     print("getting team list")
#     team_list = get_season_teams(season=season).to_dict(orient="records")
#     print(team_list)
#     return team_list


# app.run_server(debug=True)

if __name__ == "__main__":
    app.run_server(debug=True)
