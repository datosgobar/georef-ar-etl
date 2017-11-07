from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from analytics.models import Search
from geo_admin.models import Locality, State


@csrf_exempt
def save_address_search(request):
    if request.method == 'POST':
        post_data = request.POST
        search = Search(
            text=post_data.get('search_text'),
            road_name=post_data.get('road_name'),
            road_type=post_data.get('road_type'),
            door_number=post_data.get('door_number'),
            postal_code=post_data.get('postal_code'),
        )
        if post_data.get('locality'):
            try:
                search.locality = Locality.objects.get(
                    name=post_data['locality'])[0]
            except Locality.DoesNotExist:
                search.locality = None
        if post_data.get('state'):
            try:
                search.state = State.objects.get(
                    name=post_data['state'])[0]
            except State.DoesNotExist:
                search.state = None
        search.save()
        return HttpResponse('Se guardó la búsqueda correctamente.', status=201)
    return HttpResponse('El recurso no existe.', status=400)
