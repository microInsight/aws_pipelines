process CENTRIFUGER_CENTRIFUGER {
    tag "$meta.id"
    label 'process_high'

//    conda "bioconda::centrifuger=1.0.0--hdcf5f25_0"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/centrifuger:1.0.12--h077b44d_0' :
        '125434852769.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainers/centrifuger:1.0.12--h077b44d_0' }"

    input:
    tuple val(meta), path(reads)
    path(db)

    output:
    tuple val(meta), path('*results.tsv')                 , emit: results
    path "versions.yml"                                   , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def paired = "-1 ${reads[0]} -2 ${reads[1]}"
    """
    db_name=`find -L ${db} -name "*.1.cfr" -not -name "._*"  | sed 's/\\.1.cfr\$//'`

    centrifuger \\
        -x `find -L ${db} -name "*.1.cfr" -not -name "._*"  | sed 's/\\.1.cfr\$//'` \\
        $paired \\
        -t $task.cpus \\
        $args > ${prefix}.results.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        centrifuger: \$(  centrifuger -v  | sed -n 1p | sed 's/Centrifuger v//')
    END_VERSIONS
    """
}
