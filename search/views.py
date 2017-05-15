from django.http import HttpResponse
from django.shortcuts import render
from search.wrappers import APIWrapper


def search(request):
    data = {'results': {}}
    if request.is_ajax():
        address = request.GET.get('term', True)
        data = APIWrapper().search_address(address)
        return HttpResponse(data)
    return render(request, 'search.html', data)
