import json
from django.http import HttpResponse
from django.shortcuts import render
from search.wrappers import HereWrapper, NominatimWrapper


def search(request):
    data = {'results': {}}
    if request.method == 'POST':
        address = request.POST.get('address', True)
        here_wrapper = HereWrapper(
            url='https://geocoder.cit.api.here.com/6.2/geocode.json',
            app_code='2KEvGLCDtqH7HLUTNxDu3A&gen=8',
            app_id='Qop1chzvUyZdOZJatmeG')
        data['results'].update(here=here_wrapper.search_address(address))

        nominatim_wrapper = NominatimWrapper(
            url='http://nominatim.openstreetmap.org/',
            format='json',
            country_code='ar',
            address_details='1'
        )
        data['results'].update(osm=nominatim_wrapper.search_address(address))

    return render(request, 'search.html', data)


def save_address(request):
    if request.method == 'POST':
        result = request.POST.get('result', True)
        result = json.loads(result.replace('\'','"'))
        if result['source'] == 'osm':
            wrapper = NominatimWrapper(
                url=None, format=None, country_code=None, address_details=None)
        else:
            wrapper = HereWrapper(
                url=None, app_code=None, app_id=None)
        address = wrapper.get_address_from(result['address'])
        address.save()
    return HttpResponse('Se guardó la dirección correctamente.')
