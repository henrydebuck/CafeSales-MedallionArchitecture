from scripts.databaseConnection import connect_to_database
import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc

"""
Proportion Take away vs In store (pie chart)
"""
def locationPieChart(year):
   dt_pie_chart = cafe_sales[cafe_sales['year'] == year].groupby('location').size().reset_index(name='count')
   return px.pie(dt_pie_chart, title="Comparaison de location pour manger", values='count', names='location')

"""
total spent group by month (line chart)
"""
def totalSpentGroupByMonth(year):
   dt_line_chart = cafe_sales[cafe_sales['year'] == year].groupby(['month'], as_index=False)[["total_spent"]].sum()
   dt_line_chart["month"] = dt_line_chart["month"].astype(int)
   return px.line(dt_line_chart, x='month', y='total_spent', title="Argent rentrant par mois")

"""
Total of customer spent
"""
def sumTotalSpent(year):
   return cafe_sales[cafe_sales['year'] == year]['total_spent'].sum()

"""
Most popular items limited by 10 (bar chart)
"""
def mostPopularItems(year):
   dt_bar_chart = cafe_sales[cafe_sales['year'] == year].groupby(['item'], as_index=False)[['quantity']].sum().sort_values('quantity', ascending=True).iloc[:10]
   return px.bar(
      dt_bar_chart,
      x="quantity",
      y="item",
      title="10 meilleures items vendues"
   ).update_xaxes(
      tickmode="linear",
      dtick=1,
      showdividers=False
   ).update_layout(
      bargap=0.2
   )

app = Dash()

if __name__ == '__main__':
   # Get cafe_sales from database
   engine, con = connect_to_database()
   cafe_sales = pd.read_sql_table("cafe_sales", engine, schema="gold")

   # Dashboard Layout
   div1 = [
      html.Div([
         html.H2(children='CA'),
         html.H3(children=f"{sumTotalSpent(2023)}$")
      ], style={"height": 100})
   ]
   div2 = [
      dcc.Graph(figure=locationPieChart(2023))
   ]
   div3 = [
      dcc.Graph(figure=mostPopularItems(2023))
   ]

   flex_style = {'padding': 10, 'flex': 1}
   app.layout = [
      html.H1(children='2023 Cafe Sales Dashboard', style={'align-item': 'Left', 'display': 'flex', 'justify-content': 'center'}),
      html.Div(children=[
         html.Div(children=div1, style=flex_style),
         html.Div(children=div2, style=flex_style),
         html.Div(children=div3, style=flex_style)
      ], style={'display': 'flex', 'flexDirection': 'row', 'flex': '0 0 40vh'}),
      dcc.Graph(figure=totalSpentGroupByMonth(2023), style={'minHeight': 0})
   ]
   app.run(debug=True)