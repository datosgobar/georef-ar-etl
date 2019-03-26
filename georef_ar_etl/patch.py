def update_field(table, field, value, ctx, **conditions):
    ctx.report.info('Parche: update_field')
    ctx.report.info('+ Modificar las entidades que cumplan con:')
    for k, v in conditions.items():
        ctx.report.info('  > %s = %s', k, v)
    ctx.report.info('+ Establecer sus %s a: %s.', field, value)

    rows = ctx.session.query(table).filter_by(**conditions).all()
    if rows:
        for row in rows:
            setattr(row, field, value)
        ctx.report.info('+ Parche aplicado (entidades modificadas: %s).\n',
                        len(rows))
    else:
        ctx.report.warn('+ No se modificaron entidades.\n')


def apply_fn(table, fn, ctx, *expressions, **conditions):
    if bool(expressions) == bool(conditions):
        raise RuntimeError('Use only SQL expressions or keyword expressions.')

    ctx.report.info('Parche: apply_fn')
    ctx.report.info('+ Aplicar función %s a las entidades que cumplan con:',
                    fn.__name__)

    if conditions:
        for k, v in conditions.items():
            ctx.report.info('  > %s = %s', k, v)

        rows = ctx.session.query(table).filter_by(**conditions).all()
    else:
        for expr in expressions:
            ctx.report.info('  > %s', expr)

        rows = ctx.session.query(table).filter(*expressions).all()

    if rows:
        for row in rows:
            fn(row)
        ctx.report.info('+ Parche aplicado (aplicaciones: %s).\n',
                        len(rows))
    else:
        ctx.report.warn('+ No se encontraron entidades.\n')


def delete(table, ctx, **conditions):
    ctx.report.info('Parche: delete')
    ctx.report.info('+ Eliminar entidades que cumplan con:')
    for k, v in conditions.items():
        ctx.report.info('  > %s = %s', k, v)

    count = ctx.session.query(table).filter_by(**conditions).delete()
    if count:
        ctx.report.info('+ Parche aplicado (entidades eliminadas: %s).\n',
                        count)
    else:
        ctx.report.warn('+ No se borraron entidades.\n')
