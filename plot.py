# Databricks notebook source
from plotly.offline import plot
from plotly.graph_objs import *

# COMMAND ----------

data = spark.range(100)

# COMMAND ----------

display(data)

# COMMAND ----------

import plotly.plotly as py
import plotly.graph_objs as go

data = [
    go.Scatter(
        x=[0, 1, 2, 3, 4, 5, 6, 7, 8],
        y=[0, 1, 2, 3, 4, 5, 6, 7, 8]
    )
]
layout = go.Layout(
    title='Data Display',
#     font=dict(family='AG Book Ultra Light', size=18, color='#7f7f7f')
  font=dict(family='AG Book Ultra Light', size=15)
)
fig = go.Figure(data=data, layout=layout)
# plot_url = py.plot(fig, filename='global-font')
p = plot(fig, output_type='div',validate=False)
displayHTML(p)

# COMMAND ----------

import plotly.plotly as py
import plotly.graph_objs as go

data = [
    go.Scatter(
        x=[0, 1, 2, 3, 4, 5, 6, 7, 8],
        y=[0, 1, 2, 3, 4, 5, 6, 7, 8]
    )
]
layout = go.Layout(
    title='Data Display',
#     font=dict(family='AG Book Ultra Light', size=18, color='#7f7f7f')
  font=dict(family='Arctik', size=15)
)
fig = go.Figure(data=data, layout=layout)
# plot_url = py.plot(fig, filename='global-font')
p = plot(fig, output_type='div',validate=False)
displayHTML(p)

# COMMAND ----------

import plotly.plotly as py
import plotly.graph_objs as go

data = [
    go.Scatter(
        x=[0, 1, 2, 3, 4, 5, 6, 7, 8],
        y=[0, 1, 2, 3, 4, 5, 6, 7, 8]
    )
]
layout = go.Layout(
    title='Global Font',
    font=dict(family='Courier New, monospace', size=18, color='#7f7f7f')
)
fig = go.Figure(data=data, layout=layout)
# plot_url = py.plot(fig, filename='global-font')
p = plot(fig, output_type='div',validate=False)
displayHTML(p)

# COMMAND ----------


