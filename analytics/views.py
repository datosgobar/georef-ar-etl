from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from analytics.models import Search


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
            search.locality = City.objects.get_or_create(
                name=post_data['locality'].upper())[0]
        if post_data.get('state'):
            search.state = State.objects.get_or_create(
                name=post_data['state'].upper())[0]
        search.save()
        return HttpResponse('Se guardó la búsqueda correctamente.', status=201)
    return HttpResponse('El recurso no existe.', status=400)
