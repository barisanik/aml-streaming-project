/*
	# ============================================================================ #
		Macro: contains_string
	# ============================================================================ #
        Purpose: Detects if input value does not contain given character.
        Logic: Returns values without required character. dbt marks a test as failed if this query returns any rows.

        Usage on schema.yml:

            models:
            - name: model_name
                columns:
                - name: column_name
                    tests:
                    - contains_string:
                        arguments:
                            text: '-'

	# ============================================================================ #
*/
{% test contains_string(model, column_name, text) %}

SELECT {{ column_name }}
FROM {{ model }}
WHERE
    {{ column_name }} IS NOT NULL
    AND STRPOS({{ column_name }}, '{{ text }}') = 0

{% endtest %}