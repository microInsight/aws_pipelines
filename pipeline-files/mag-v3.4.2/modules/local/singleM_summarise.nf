process SINGLEM_SUMMARISE {
    tag "${meta.id}"
    label 'process_low'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
    'https://depot.galaxyproject.org/singularity/singlem:0.18.3--pyhdfd78af_0' :
    'quay.io/biocontainers/singlem:0.20.3--pyhdfd78af_1' }"

    input:
    tuple val(meta), path(singlem_profile), path(singlem_otu_tables)
    val (taxonomic_level)

    output:
    tuple val(meta), path("*.csv") , emit: singleM_levelsxsite
    tuple val(meta), path("*.tsv") , emit: singleM_longprofile
    tuple val(meta), path("*.html"), emit: singleM_otu_comm
    path "versions.yml"            , emit: versions

    script:
    def args = task.ext.args ?: ''

    """
    singlem summarise \\
        --input-taxonomic-profile ${singlem_profile} \\
        --output-species-by-site-relative-abundance ${meta.id}_${taxonomic_level}_by_site.csv \\
        --output-species-by-site-level ${taxonomic_level}

    singlem summarise \\
        --input-taxonomic-profile ${singlem_profile} \\
        --output-taxonomic-profile-with-extras ${meta.id}_profile_with_extras.tsv

    singlem summarise \\
        --input-otu-table ${singlem_otu_tables} \\
        --output-otu-table ${meta.id}_combined_otu_table.csv

    singlem summarise \\
        --input-otu-table ${singlem_otu_tables} \\
        --krona ${meta.id}_otu_krona.html



    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        singleM: \$(singlem pipe --version)
    END_VERSIONS
    """
}
