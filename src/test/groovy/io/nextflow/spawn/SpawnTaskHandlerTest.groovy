package io.nextflow.spawn

import spock.lang.Specification

class SpawnTaskHandlerTest extends Specification {

    def 'passes the task script via --user-data-file, not a bare --user-data path (#13)'() {
        when:
        def cmd = SpawnTaskHandler.buildLaunchCommand(
            'nf-abc123', 't3.medium', 'us-east-1', '2h', false, '/tmp/nf-spawn-abc.sh')

        then:
        // The script path must be carried by --user-data-file so spawn reads
        // the file's contents. A bare --user-data <path> would be treated as
        // inline user-data and the script would never run on the instance.
        cmd.contains('--user-data-file')
        cmd[cmd.indexOf('--user-data-file') + 1] == '/tmp/nf-spawn-abc.sh'

        and: 'the broken form must not reappear'
        !cmd.contains('--user-data')

        and: 'launch does not block on running/ssh'
        cmd.contains('--wait-for-running=false')
        cmd.contains('--wait-for-ssh=false')
    }

    def 'adds --spot only when requested'() {
        expect:
        SpawnTaskHandler.buildLaunchCommand('n', 't3.medium', 'us-east-1', '2h', spot, '/s.sh')
            .contains('--spot') == expected

        where:
        spot  | expected
        true  | true
        false | false
    }
}
