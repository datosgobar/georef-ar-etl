import json
from django.http import HttpResponse
from django.shortcuts import render, redirect
from search.wrappers import PostgresWrapper


def search(request):
    data = {'results': {}}
    if request.method == 'POST':
        address = request.POST.get('address', True)
        wrapper = PostgresWrapper()
        data['suggestions'] = wrapper.search_address(address)

    return render(request, 'search.html', data)


def save_address(request):
    if request.method == 'POST':
        search_text = request.POST.get('search_text', True)
        result = request.POST.get('result', True)
        result = json.loads(result.replace('\'', '"'))
        if result['source'] == 'osm':
            wrapper = NominatimWrapper(
                url=None, format=None, country_code=None, address_details=None)
        else:
            wrapper = HereWrapper(
                url=None, app_code=None, app_id=None)
        address = wrapper.get_address_from(search_text, result['address'])
        address.save()
        return HttpResponse('Se guardó la dirección correctamente.')
    return redirect('search')
