from flask import Flask, render_template, request, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_wtf import Form
from wtforms import DateField, Field, widgets
from wtforms.validators import DataRequired, ValidationError, AnyOf

from db_handler import process

app = Flask(__name__)
Bootstrap(app)
app.config['SECRET_KEY'] = 'DontTellAnyone'
cities = ['Ankara', 'Astana', 'Bishkek', 'Ekaterinburg', 'Kaliningrad',
          'Kazan', 'Kiev', 'Minsk', 'Moscow', 'Novosibirsk', 'Penza', 'Saint-Petersburg', 'Tver', 'Ufa', 'Voronezh']


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def check_presence(form, field):
    if field.data not in cities:
        raise ValidationError("city must be present in cities list")


class TagListField(Field):
    widget = widgets.TextInput()

    def _value(self):
        if self.data:
            return u', '.join(self.data)
        else:
            return u''

    def process_formdata(self, valuelist):
        if valuelist:
            self.data = [x.strip() for x in valuelist[0].split(',')]
        else:
            self.data = []


class LoginForm(Form):
    city = TagListField('enter city (comma separated)', validators=[DataRequired(), AnyOf(values=cities,
                                                                        message="wrong city")])
    plan_start = DateField('start date', validators=[DataRequired()], format='%Y-%m-%d')
    plan_end = DateField('end date', validators=[DataRequired()], format='%Y-%m-%d')


class Result:
    def __init__(self, name):
        self.name = name


@app.route('/', methods=['GET', 'POST'])
def index():
    form = LoginForm()
    if form.validate_on_submit():
        print(form.city.data)
        return redirect(url_for('handle_data'))
    return render_template('index.html', form=form)


@app.route('/handle_data', methods=['GET', 'POST'])
def handle_data():
    city_form = request.form.get("city")
    city_form_2 = city_form.replace(" ", "").split(",")
    start_form = request.form.get("plan_start")
    end_form = request.form.get("plan_end")
    print("city_form_2", "start_form", "end_form")
    out = process(city_form_2, start_form, end_form)
    lst_of_cities, lst_of_values = [], []
    for key, value in out.items():
        lst_of_cities.append(key)
        lst_of_values.append(value)

    return render_template('handle_data.html', data=dict(zip(lst_of_cities, lst_of_values)))


if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)
