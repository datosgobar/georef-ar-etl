def update_field(table, field, value, ctx, **conditions):
    ctx.logger.info('Parche: update_field')
    ctx.logger.info('+ Modificar las entidades que cumplan con:')
    for k, v in conditions.items():
        ctx.logger.info('  > %s = %s', k, v)
    ctx.logger.info('+ Establecer sus %s a: %s.', field, value)

    rows = ctx.query(table).filter_by(**conditions).all()
    if rows:
        for row in rows:
            setattr(row, field, value)
        ctx.logger.info('+ Parche aplicado (entidades modificadas: %s).',
                        len(rows))
    else:
        ctx.logger.warn('+ No se modificaron entidades.')

    ctx.logger.info('')


def apply_fn(table, fn, ctx, **conditions):
    ctx.logger.info('Parche: apply_fn')
    ctx.logger.info('+ Aplicar función %s a las entidades que cumplan con:',
                    fn.__name__)
    for k, v in conditions.items():
        ctx.logger.info('  > %s = %s', k, v)

    rows = ctx.query(table).filter_by(**conditions).all()
    if rows:
        for row in rows:
            fn(row)
        ctx.logger.info('+ Parche aplicado (aplicaciones: %s).',
                        len(rows))
    else:
        ctx.logger.warn('+ No se encontraron entidades.')

    ctx.logger.info('')


def delete(table, ctx, **conditions):
    ctx.logger.info('Parche: delete')
    ctx.logger.info('+ Eliminar entidades que cumplan con:')
    for k, v in conditions.items():
        ctx.logger.info('  > %s = %s', k, v)

    count = ctx.query(table).filter_by(**conditions).delete()
    if count:
        ctx.logger.info('+ Parche aplicado (entidades eliminadas: %s).', count)
    else:
        ctx.logger.warn('+ No se borraron entidades.')

    ctx.logger.info('')
