from django.http import HttpResponse
from django.shortcuts import render, redirect
from search.wrappers import PostgresWrapper


def search(request):
    data = {'results': {}}
    if request.is_ajax():
        address = request.GET.get('term', True)
        wrapper = PostgresWrapper()
        data = wrapper.search_address(address)
        return HttpResponse(data)

    return render(request, 'search.html', data)
