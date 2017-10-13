from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from benchmark.wrappers import KongWrapper


@login_required
def get_token(request):
    data = {}
    if request.method == 'POST':
        username = request.POST.get('username')
        kong = KongWrapper()
        credentials = kong.get_credentials(username)
        data['token'] = kong.get_token_for(credentials)
    return render(request, 'token.html', data)
