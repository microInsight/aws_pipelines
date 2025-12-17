process TAXPASTA_STANDARDISE {
    tag "${meta.id}"
    label 'process_single'

    conda "${moduleDir}/environment.yml"
    container "${workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container
        ? 'biocontainers/taxpasta:0.7.0--pyhdfd78af_1'
        : '125434852769.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainers/taxpasta:0.7.0--pyhdfd78af_1'}"

    input:
    tuple val(meta), path(profile)
    path taxonomy

    output:
    tuple val(meta), path("*.{tsv,csv,arrow,parquet,biom}"), emit: standardised_profile
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def taxonomy_option = taxonomy ? "--taxonomy ${taxonomy}" : ''
    """
    taxpasta standardise \\
        ${args} \\
        ${taxonomy_option} \\
        ${profile}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        taxpasta: \$(taxpasta --version)
    END_VERSIONS
    """

}
