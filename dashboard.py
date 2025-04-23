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
        
        # Create all figures
        #Geospatial Heatmap
        satisfaction_data=load_data('satisfaction_map')
        fig1=px.choropleth(
            satisfaction_data,
            locations="country_code",
            color="avg_satisfaction",
            hover_name="country",
            hover_data=["count"],
            title="Job Satisfaction by Country",
            color_continuous_scale=px.colors.sequential.Plasma
        )
        
        #Radar Chart
        gender_data=load_data('satisfaction_radar')
        fig2 = px.line_polar(
            gender_data,
            r="avg_value",
            theta="satisfaction_level",
            color="gender",
            line_close=True,
            title="Job Satisfaction Levels by Gender"
        )
        
        #Parallel Coordinates
        stress_data=load_data('parallel_coords')
        stress_level_map={'Low':0,'Medium':1,'High':2}
        stress_data['stress_numeric']=stress_data['stress_level'].map(stress_level_map)

        fig3=px.parallel_coordinates(
            stress_data,
            color="stress_numeric",
            dimensions=["avg_sleep", "avg_work_hours", "avg_work_life_balance"],
            title="Stress Level vs Sleep, Work Hours and Work-Life Balance",
            color_continuous_scale=px.colors.sequential.Viridis,
            labels={
                "avg_sleep":"Avg Sleep Hours",
                "avg_work_hours":"Avg Work Hours", 
                "avg_work_life_balance":"Work-Life Balance",
                "stress_numeric":"Stress Level"
            }
        )

        fig3.update_layout(
            coloraxis_colorbar=dict(
                title="Stress Level",
                tickvals=[0, 1, 2],
                ticktext=["Low", "Medium", "High"]
            )
        )
        
        #Bubble Chart
        work_life_data=load_data('work_life_bubble')
        fig4=px.scatter(
            work_life_data,
            x="avg_work_hours",
            y="avg_sleep",
            size="count",
            color="stress_level",
            title="Work Hours vs Sleep Hours by Stress Level"
        )
        #Health Risk Barchart
        health_risk_data=load_data('health_risk_count_bar')
        fig5=px.bar(
            health_risk_data,
            y="health_risk_group",
            x="count",
            orientation="h" ,
            color="health_risk_group",
            title="Number of Individuals by Health Risk Group",
            labels= {
                "health_risk_group":"Health Risk Group",
                "count":"Number of Individuals"
            }
            
        )
        
         #Sunburst Chart
        employment_data =load_data('employment_sunburst')
        fig6=px.sunburst(
            employment_data,
            path =['full_or_part_time', 'salary_or_hourly'],
            values='count' ,
            title='Employment Type Distribution'
        )
        
          #Violin Plot
        salary_data=load_data('salary_violin')
        fig7=px.violin(
            salary_data,
            y="avg_salary",
            x="department",
            box=True,
            points="all",
            title="Salary Distribution by Department"
            
        )
        
        #Bar Chart
        salary_dept_data =load_data('salary_dept_stack')
        
        fig8=px.bar(
            salary_dept_data,
            x="dept_size_category",
            y="avg_annual_salary",
            color="job_level",
            barmode="stack",
            title="Average Annual Salary by Department Size and Job Level",
            labels={
                "dept_size_category":"Department Size",
                "avg_annual_salary":"Average Annual Salary",
                "job_level":"Job Level"
            }
        )
        
        #Work Hours Bar
        work_hours_data=load_data('work_hours_bar')
        fig9=px.bar(
            work_hours_data,
            x="work_hours_group",
            y="avg_severity",
            color="avg_work_life_balance",
            title="Mental Health Severity by Work Hours Group"
            
        )
                
    
        


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
        
