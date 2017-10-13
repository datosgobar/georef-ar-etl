from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from geo_admin import helpers


@login_required
def get_token(request):
    data = {}
    if request.method == 'POST':
        consumer = helpers.get_consumer(request.user.username)
        data['token'] = helpers.get_token_for(consumer)
    return render(request, 'token.html', data)
