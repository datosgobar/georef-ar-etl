from django.http import HttpResponse
from django.shortcuts import render, redirect
from benchmark.wrappers import GeorefWrapper, HereWrapper, NominatimWrapper
import json


def search(request):
    data = {'results': {}}
    if request.is_ajax():
        address = request.GET.get('term')
        data = GeorefWrapper().search_address(address)
        return HttpResponse(data)
    return render(request, 'search.html', data)


def normalize(request):
    data = {'results': None}
    wrapper = GeorefWrapper()
    if request.method == 'POST':
        address = request.POST.get('address')
        locality = request.POST.get('locality')
        state = request.POST.get('state')
        data['results'] = json.loads(
            wrapper.search_address(address, locality, state))
    data['states'] = json.loads(wrapper.get_states())
    data['localities'] = json.loads(wrapper.get_localities())
    return render(request, 'normalize.html', data)


def compare(request):
    data = {'results': {}}
    if request.method == 'POST':
        address = request.POST.get('address', True)
        data['results'].update(here=HereWrapper().search_address(address))
        data['results'].update(osm=NominatimWrapper().search_address(address))
        georef_data = json.loads(
            GeorefWrapper().search_address(address).decode('utf-8'))
        data['results'].update(georef=georef_data)
    return render(request, 'compare.html', data)


def save_address(request):
    if request.method == 'POST':
        search_text = request.POST.get('search_text', True)
        result = request.POST.get('result', True)
        result = json.loads(result.replace('\'', '"'))
        if result['source'] == 'georef':
            wrapper = GeorefWrapper()
        elif result['source'] == 'osm':
            wrapper = NominatimWrapper(
                url=None, format=None, country_code=None, address_details=None)
        else:
            wrapper = HereWrapper(
                url=None, app_code=None, app_id=None)
        address = wrapper.get_address_from(search_text, result['address'])
        address.save()
        return HttpResponse('Se guardó la dirección correctamente.')
    return redirect('compare')
