from flask import Flask, render_template, request, url_for, redirect
import os
import io, base64
import matplotlib.pyplot as plt
from cloud import save_all, get_all
from graph import make_fig

app = Flask(__name__)

@app.route('/', methods=["GET",'POST'])
def hello_world():
    choices = ["heatmap","hist"]
    choices2 = ["bigquery","azure"]
    if request.method=="POST":
        graph=request.form.get('choices',None)
        vendor=request.form.get('vendors',None)
        source=request.form.get('source',None)
        if graph is not None:
            plt.clf()
            output = io.BytesIO()
            dataset=get_all(vendor)
            make_fig(dataset,graph)
            plt.savefig(output,format='png')
            plot_url = base64.b64encode(output.getvalue()).decode() 
            return '<img src="data:image/png;base64,{}">'.format(plot_url) 
        elif source is not None:
            save_all(source)
            return "База обновлена"
    return render_template('index.html', choices=choices, choices2=choices2, vendors=choices2)

if __name__ == '__main__':
    app.run()