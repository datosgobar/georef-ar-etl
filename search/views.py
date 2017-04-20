from django.shortcuts import render, redirect
from search.wrappers import PostgresWrapper


def search(request):
    data = {'results': {}}
    if request.method == 'POST':
        address = request.POST.get('address', True)
        wrapper = PostgresWrapper()
        data['suggestions'] = wrapper.search_address(address)

    return render(request, 'search.html', data)
