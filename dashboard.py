import dash
import matplotlib
matplotlib.use("Agg")
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
import os
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path


def create_dashboard():
    app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
    try:
        # Load all data files
        def load_data(filename):
            # Handle both cases - with and without .png extension
            if not filename.endswith('.png'):
                filename += '.png'

            # Define the data file path
            data_file = filename.replace('.png', '_data.csv')
            data_path = Path(__file__).parent.parent / "report_images" / data_file

            # Debugging the output


            if not data_path.exists():
                available_files = "\\n".join(os.listdir(Path(__file__).parent.parent / "report_images"))
                raise FileNotFoundError(
                    f"Data file not found at: {data_path}\\n"

                    f"Available files:\\n{available_files}"
                )

            return pd.read_csv(data_path)
        


          # need to add the figures here


         # Set up layout with tabs
        app.layout = dbc.Container([
            html.H1("Workforce Analytics: Employment, Satisfaction & Mental Health", className="mb-4 text-center"),
            
            dcc.Tabs([
                # Tab 1: Regional Trends
                dcc.Tab(label='Regional Trends', children=[
                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=fig1), width=12)
                    ]),
                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=fig2), width=6),
                        dbc.Col(dcc.Graph(figure=fig6), width=6)
                    ])
                ]),
                
                # Tab 2: Mental Health
                dcc.Tab(label='Mental Health', children=[
                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=fig3), width=12)
                    ]),
                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=fig4), width=6),
                        dbc.Col(dcc.Graph(figure=fig5), width=6)
                    ]),
                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=fig9), width=12)
                    ])
                ]),
                
                # Tab 3: Compensation
                dcc.Tab(label='Compensation', children=[
                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=fig7), width=12)
                    ]),
                    dbc.Row([
                        dbc.Col(dcc.Graph(figure=fig8), width=12)
                    ])
                ])
                
            ])
        ], fluid=True)
        

      
        
    except Exception as e:
        app.layout = html.Div([
            html.H1("Error Loading Dashboard"),
            html.P(str(e)),
            html.P("Please run the pipeline first to generate required data files.")
        ])
    
    return app

if __name__ == '__main__':
    app = create_dashboard()
    app.run(debug=True)
        
