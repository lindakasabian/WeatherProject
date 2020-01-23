import psycopg2
from flask import Flask, render_template, flash
from flask_wtf import Form
from wtforms import StringField, PasswordField, DateField, Field, widgets
from wtforms.validators import InputRequired, Email, Length, AnyOf, DataRequired
from flask_bootstrap import Bootstrap
from db_handler import *


app = Flask(__name__)
Bootstrap(app)
app.config['SECRET_KEY'] = 'DontTellAnyone'


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

    city = TagListField('enter city', validators=[DataRequired()])
    plan_start = DateField('start date', validators=[DataRequired()], format='%Y-%m-%d')
    plan_end = DateField('end date', validators=[DataRequired()], format='%Y-%m-%d')


#    password = PasswordField('password',
#                             validators=[InputRequired(), Length(min=5, max=10), AnyOf(['secret', 'password'])])


@app.route('/', methods=['GET', 'POST'])
def index():
    form = LoginForm()
    if form.validate_on_submit():

        city = form.city.data
        start_date = form.plan_start.data
        end_date = form.plan_end.data
    return render_template('index.html', form=form)


if __name__ == '__main__':
    app.run(debug=True)
