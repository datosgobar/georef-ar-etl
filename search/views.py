from django.shortcuts import render
from search.wrappers import HereWrapper, NominatimWrapper


def search(request):
    data = {}
    if request.method == 'POST':
        address = request.POST.get('address', True)
        here_wrapper = HereWrapper(
            url='https://geocoder.cit.api.here.com/6.2/geocode.json',
            app_code='2KEvGLCDtqH7HLUTNxDu3A&gen=8',
            app_id='Qop1chzvUyZdOZJatmeG')
        data['here_data'] = here_wrapper.search_address(address)

        nominatim_wrapper = NominatimWrapper(
            url='http://nominatim.openstreetmap.org/',
            format='json',
            polygon='1',
            address_details='1'
        )
        data['osm_data'] = nominatim_wrapper.search_address(address)

    return render(request, 'search.html', data)

