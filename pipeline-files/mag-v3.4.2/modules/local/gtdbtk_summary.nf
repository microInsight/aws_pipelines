process GTDBTK_SUMMARY {


    conda "conda-forge::pandas=1.4.3"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/pandas:1.4.3' :
        'biocontainers/pandas:1.4.3' }"

    input:
    path(qc_discarded_bins)
    path(gtdbtk_summaries)
    path(filtered_bins)
    path(failed_bins)

    output:
    path "gtdbtk_summary.tsv", emit: summary
    path "versions.yml"      , emit: versions

    script:
    def args = task.ext.args ?: ''
    def discarded = qc_discarded_bins.sort().size() > 0 ? "--qc_discarded_bins ${qc_discarded_bins}" : ""
    def summaries = gtdbtk_summaries.sort().size() > 0 ?  "--summaries ${gtdbtk_summaries}" : ""
    def filtered  = filtered_bins.sort().size() > 0 ?     "--filtered_bins ${filtered_bins}" : ""
    def failed    = failed_bins.sort().size() > 0 ?       "--failed_bins ${failed_bins}" : ""
    """
    chmod +x /mnt/workflow/definition/mag-v3.4.2/bin/summary_gtdbtk.py

    if [ -z "$args" ] && [ -z "$discarded" ] && [ -z "$summaries" ] && [ -z "$filtered" ] && [ -z "$failed" ]; then
        echo "No input files provided to GTDBTK_SUMMARY. Exiting without running summary script."
        echo -e "summary:\nversions:" > versions.yml
        exit 0
    else
        python3 /mnt/workflow/definition/mag-v3.4.2/bin/summary_gtdbtk.py $args $discarded $summaries $filtered $failed --out gtdbtk_summary.tsv
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version 2>&1 | sed 's/Python //g')
        pandas: \$(python -c "import pkg_resources; print(pkg_resources.get_distribution('pandas').version)")
    END_VERSIONS
    """
}
