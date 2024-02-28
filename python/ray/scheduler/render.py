
from typing import List, Optional
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta




class TableRenderer:
    """
    Convert a pandas DataFrame into an HTML table.
    """

    def render(self, df: pd.DataFrame, header_labels: Optional[List] = None, table_width: Optional[int] = None) -> str:
        # Check if custom labels are provided and have the correct length
        if header_labels is not None and len(header_labels) == len(df.columns):
            df = df.copy()
            df.columns = header_labels

        style = f"""
            <style>
                .table-class {{
                    border: 1px solid #ccc;  /* Add a thin border around the table */
                    border-collapse: collapse;
                    font-family: Arial, sans-serif;
                    color: #333;
                    {f'width: {table_width}px;' if table_width is not None else ''}
                }}
                .table-class th, .table-class td {{
                    border: 1px solid #ccc;  /* Add a thin border around each cell */
                    padding: 8px;  /* Add some padding inside each cell */
                }}
                /* Set the background color for even rows */
                .table-class tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                /* Add a hover effect to the rows */
                .table-class tr:hover {{
                    background-color: #ddd;
                }}
                /* Center the column headers */
                .table-class th {{
                    text-align: center;
                }}
            </style>
        """

        return style + df.to_html(classes="table-class", index=False)


class GanttChartRenderer:
    """
    This renderer is primarily used by the timeline deck. The input DataFrame should
    have at least the following columns:
    - "Start": datetime.datetime (represents the start time)
    - "Finish": datetime.datetime (represents the end time)
    - "Name": string (the name of the task or event)
    """

    def render(self, df: pd.DataFrame, chart_width: Optional[int] = None) -> str:
        fig = px.timeline(df, x_start="Start", x_end="Finish", y="Name", color="Name", width=chart_width)

        fig.update_xaxes(
            # tickangle=90,
            rangeslider_visible=True,
            tickformatstops=[
                dict(dtickrange=[None, 1], value="%3f ms"),
                dict(dtickrange=[1, 60], value="%S:%3f s"),
                dict(dtickrange=[60, 3600], value="%M:%S m"),
                dict(dtickrange=[3600, None], value="%H:%M h"),
            ],
        )

        # Remove y-axis tick labels and title since the time line deck space is limited.
        fig.update_yaxes(showticklabels=False, title="")

        fig.update_layout(
            autosize=True,
            # Set the orientation of the legend to horizontal and move the legend anchor 2% beyond the top of the timeline graph's vertical axis
            legend=dict(orientation="h", y=1.02),
        )

        # fig.show()
        return fig.to_html()




