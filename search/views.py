from django.shortcuts import render


def search(request):
    data = {}
    if request.method == 'POST':
        address = request.POST.get('address', True)
        data['results'] = 'No se encontró la dirección.'
    else:
        data['results'] = None

    return render(request, 'search.html', data)

