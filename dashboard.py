import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
import psycopg2
import psycopg2.extras
#import main
from config import config

# Database connection
conn = psycopg2.connect(
    host="localhost", port="5432", database="main_homicide_database",
    user="postgres", password="Khiz1234")

# Define options
provinces = {
    'Western Cape': ['Cape Town', 'Stellenbosch', 'George'],
    'Eastern Cape': ['Port Elizabeth', 'East London', 'Grahamstown'],
    'Gauteng': ['Johannesburg', 'Pretoria', 'Soweto'],
    # Add more provinces and towns here
}

race_options = [
    {'label': 'African', 'value': 'African'},
    {'label': 'White', 'value': 'White'},
    {'label': 'Coloured', 'value': 'Coloured'},
    {'label': 'Indian/Asian', 'value': 'Indian/Asian'},
    {'label': 'Other', 'value': 'Other'}
]

relationship_options = [
    {'label': 'Family', 'value': 'Family'},
    {'label': 'Friend', 'value': 'Friend'},
    {'label': 'Acquaintance', 'value': 'Acquaintance'},
    {'label': 'Stranger', 'value': 'Stranger'},
    {'label': 'Other', 'value': 'Other'}
]

bool_options = [
    {'label': 'Yes', 'value': True},
    {'label': 'No', 'value': False}
]

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.H2("Homicide Data Entry"),
    
      dbc.Row([
    #     dbc.Col([
    #         dbc.Label("ID"),
    #         dbc.Input(id='id-input', type='number', placeholder="Enter ID"),
    #     ], width=6),
        
        dbc.Col([
            dbc.Label("News Report URL"),
            dbc.Input(id='url-input', type='text', placeholder="Enter news report URL"),
        ], width=6),
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Label("News Platform"),
            dbc.Input(id='outlet-input', type='text', placeholder="Enter news outlet"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Date of Publication"),
            dbc.Input(id='publication-date-input', type='date'),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Author"),
            dbc.Input(id='author-input', type='text', placeholder="Enter author name"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Headline"),
            dbc.Input(id='headline-input', type='text', placeholder="Enter headline"),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Victim Name"),
            dbc.Input(id='victim-name-input', type='text', placeholder="Enter victim name"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Date of Death"),
            dbc.Input(id='death-date-input', type='date'),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Age of Victim"),
            dbc.Input(id='victim-age-input', type='number', placeholder="Enter victim age"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Race of Victim"),
            dcc.Dropdown(id='race-dropdown', 
                         options=race_options,
                         placeholder="Select race"),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Type of Location"),
            dbc.Input(id='location-type-input', type='text', placeholder="Enter type of location"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Province"),
            dcc.Dropdown(id='province-dropdown', 
                         options=[{'label': k, 'value': k} for k in provinces.keys()],
                         placeholder="Select a province"),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Town"),
            dcc.Dropdown(id='town-dropdown', 
                         placeholder="Select a town"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Sexual Assault"),
            dcc.Dropdown(id='sexual-assault-dropdown', 
                         options=bool_options,
                         placeholder="Select option"),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Mode of Death"),
            dbc.Input(id='mode-of-death-input', type='text', placeholder="Enter mode of death"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Robbery"),
            dcc.Dropdown(id='robbery-dropdown', 
                         options=bool_options,
                         placeholder="Select option"),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Suspect Arrested"),
            dcc.Dropdown(id='suspect-arrested-dropdown', 
                         options=bool_options,
                         placeholder="Select option"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Suspect Convicted"),
            dcc.Dropdown(id='suspect-convicted-dropdown', 
                         options=bool_options,
                         placeholder="Select option"),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Perpetrator Name"),
            dbc.Input(id='perp-name-input', type='text', placeholder="Enter perpetrator name"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Perp Relationship"),
            dcc.Dropdown(id='relationship-dropdown', 
                         options=relationship_options,
                         placeholder="Select relationship"),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Multiple Murders"),
            dcc.Dropdown(id='multi-murder-dropdown', 
                         options=bool_options,
                         placeholder="Select option"),
        ], width=6),
        
        dbc.Col([
            dbc.Label("Extreme Violence"),
            dcc.Dropdown(id='extreme-violence-dropdown', 
                         options=bool_options,
                         placeholder="Select option"),
        ], width=6),
    ]),

    dbc.Row([
        dbc.Col([
            dbc.Label("Notes"),
            dbc.Textarea(id='notes-input', placeholder="Enter additional notes"),
        ], width=12),
    ]),

    dbc.Button("Submit", id="submit-button", color="primary", className="mt-3"),
    html.Div(id="output-message", className="mt-3"),
])

# Callback for updating town options based on selected province
@app.callback(
    Output('town-dropdown', 'options'),
    Input('province-dropdown', 'value')
)
def set_town_options(selected_province):
    return [{'label': town, 'value': town} for town in provinces.get(selected_province, [])]

# Callback for handling form submission
@app.callback(
    Output('output-message', 'children'),
    Input('submit-button', 'n_clicks'),
    # State('id-input', 'value'),
    State('url-input', 'value'),
    State('outlet-input', 'value'),
    State('publication-date-input', 'value'),
    State('author-input', 'value'),
    State('headline-input', 'value'),
    State('victim-name-input', 'value'),
    State('death-date-input', 'value'),
    State('victim-age-input', 'value'),
    State('race-dropdown', 'value'),
    State('location-type-input', 'value'),
    State('province-dropdown', 'value'),
    State('town-dropdown', 'value'),
    State('sexual-assault-dropdown', 'value'),
    State('mode-of-death-input', 'value'),
    State('robbery-dropdown', 'value'),
    State('suspect-arrested-dropdown', 'value'),
    State('suspect-convicted-dropdown', 'value'),
    State('perp-name-input', 'value'),
    State('relationship-dropdown', 'value'),
    State('multi-murder-dropdown', 'value'),
    State('extreme-violence-dropdown', 'value'),
    State('notes-input', 'value')
)
def submit_form(n_clicks, url, outlet, pub_date, author, headline, victim_name, death_date,
                victim_age, race, location_type, province, town, sexual_assault, mode_of_death, 
                robbery, suspect_arrested, suspect_convicted, perp_name, relationship,
                multi_murder, extreme_violence, notes):
    # if notes == "NA":
    #     notes = "No notes provided"
    # print(f"Notes value: {notes}")
    if n_clicks is None:
        return ""

    # Prepare the SQL insert statement
    cur = conn.cursor()
    insert_query_article = '''INSERT INTO homicide_media.homicide_article
                     (news_report_url, news_report_headline, news_report_platform, date_of_publication, author,  notes)
                     VALUES (%s, %s, %s, %s, %s, %s)'''
    
    values = (url, headline, outlet, pub_date, author, notes)
    
    insert_query_victim = '''INSERT INTO homicide_victim.victim(
                        news_report_url, victim_name, date_of_death, place_of_death_province, place_of_death_town, type_of_location,
                        sexual_assault, race_of_victim, age_of_victim, mode_of_death_specific, robbery_y_n_u)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    values_victim= (url,victim_name, death_date, province, town, location_type, sexual_assault, race, victim_age,mode_of_death,robbery )
    
    insert_query_perpetrator = '''INSERT INTO homicide_perpetrator.perpetrator(
                            news_report_url, perpetrator_name, perpetrator_relationship_to_victim, suspect_arrested, suspect_convicted, serial_killer, extreme_violence_y_n_m_u)
                            VALUES(%s,%s,%s,%s,%s,%s,%s)'''
    values_perpetrator = (url,perp_name, relationship, suspect_arrested, suspect_convicted,multi_murder, extreme_violence)
    
    try:
        cur.execute(insert_query_article, values)
        cur.execute(insert_query_victim, values_victim)
        cur.execute(insert_query_perpetrator, values_perpetrator)
        conn.commit()
        return "Data successfully inserted!"
    except Exception as e:
        conn.rollback()
        return f"Error: {str(e)}"

if __name__ == '__main__':
    app.run_server(debug=True)