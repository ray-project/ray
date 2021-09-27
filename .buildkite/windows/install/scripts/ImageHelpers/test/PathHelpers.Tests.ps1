. $PSScriptRoot\..\PathHelpers.ps1

Describe 'Test-MachinePath Tests' {
    Mock Get-MachinePath {return "C:\foo;C:\bar"}
    It 'Path contains item' {
        Test-MachinePath -PathItem "C:\foo" | Should Be $true
    }
    It 'Path does not containe item' {
        Test-MachinePath -PathItem "C:\baz" | Should Be $false
    }
}

Describe 'Set-MachinePath Tests' {
    Mock Get-MachinePath {return "C:\foo;C:\bar"}
    Mock Set-ItemProperty {return}
    It 'Set-MachinePath should return new path' {
        Set-MachinePath -NewPath "C:\baz" | Should Be "C:\baz"
    }
}

Describe "Add-MachinePathItem Tests"{
    Mock Get-MachinePath {return "C:\foo;C:\bar"}
    Mock Set-ItemProperty {return}
    It 'Add-MachinePathItem should return complete path' {
        Add-MachinePathItem -PathItem 'C:\baz' | Should Be 'C:\baz;C:\foo;C:\bar'
    }
}

Describe 'Set-SystemVariable Tests' {
    Mock Set-ItemProperty {return}
    It 'Set-SystemVariable should return new path' {
        Set-SystemVariable -SystemVariable "NewPathVar" -Value "C:\baz" | Should Be "C:\baz"
    }
}
