#!groovy

build('dmt_client', 'docker-host') {

  checkoutRepo()

  runStage('compile') {
    withGithubSshCredentials {
      sh 'git submodule update --init'
    }
  }

  def pipeDefault
  runStage('load pipeline') {
    env.JENKINS_LIB = "builtils/jenkins_lib"
    pipeDefault = load("${env.JENKINS_LIB}/pipeDefault.groovy")
  }

  pipeDefault() {
    runStage('compile') {
      withGithubPrivkey {
        sh 'make wc_compile'
      }
    }
    runStage('lint') {
      sh 'make wc_lint'
    }
    runStage('xref') {
      sh 'make wc_xref'
    }
  }

}
