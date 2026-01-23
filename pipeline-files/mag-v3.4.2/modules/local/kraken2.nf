process KRAKEN2 {
    tag "${meta.id}-${db}"
    label 'process_high'

    conda "bioconda::kraken2=2.17.1"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/kraken2:2.17.1--pl5321h077b44d_0' :
        'biocontainers/kraken2:2.17.1--pl5321h077b44d_0' }"

    input:
    tuple val(meta), path(reads)
    path(db)

    output:
    tuple val("kraken2"), val(meta), path("results.krona"), emit: results_for_krona
    tuple val(meta), path("*kraken2_report.txt")          , emit: report
    tuple val(meta), path('*.classified{.,_}*')           , optional:true, emit: classified_reads_fastq
    tuple val(meta), path('*.unclassified{.,_}*')         , optional:true, emit: unclassified_reads_fastq
    path "versions.yml"                                   , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ""
    def prefix = task.ext.prefix ?: "${meta.id}"
    def filenames = "--paired \"${reads[0]}\" \"${reads[1]}\""
    prefix = task.ext.prefix ?: "${meta.id}"

    """
    k2 classify \
        --report-zero-counts \
        --threads ${task.cpus} \
        --db ${db} \
        --report ${prefix}.kraken2_report.txt \
        --memory-mapping \
        --use-names \
        --minimum-hit-groups 4 \
        --confidence 0.05 \
        --classified-out ${prefix}#_classified_reads.fq \
        --unclassified-out ${prefix}#_unclassified_reads.fq \
        $args \
        $filenames

        > kraken2.kraken
    cat kraken2.kraken | cut -f 2,3 > results.krona

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        kraken2: \$(echo \$(kraken2 --version 2>&1) | sed 's/^.*Kraken version //' | sed 's/ Copyright.*//')
    END_VERSIONS
    """
}
