# Define the directory containing the CSV file and the input and output file names
$inputDirectory = "C:\temp\awards"  # Change this to your directory
$outputDirectory = Join-Path -Path $inputDirectory -ChildPath "out"

# Create the output directory if it does not exist
if (-not (Test-Path -Path $outputDirectory)) {
    New-Item -Path $outputDirectory -ItemType Directory
}

# Get all CSV files in the specified directory
$csvFiles = Get-ChildItem -Path $inputDirectory -Filter *.csv

# Define the list of Product or Service Codes to filter
$codesToFilter = @("R499", "D399", "D306", "R408", "R410", "D308", "D318", "D301", "DC01", "DA01")

# Convert the codes to a HashSet for faster lookup
$codesHashSet = [System.Collections.Generic.HashSet[string]]::new()
$codesToFilter | ForEach-Object { $codesHashSet.Add($_) }

foreach ($csvFile in $csvFiles) {

    Write-Host "Now processing...$csvFile"

     # Start timing the processing
     $startTime = Get-Date

    # Create the full path for the input file
    $inputFilePath = $csvFile.FullName
    
    # Generate the output filename by appending "_subset" to the input filename
    $outputFileName = [System.IO.Path]::GetFileNameWithoutExtension($csvFile.Name) + "_subset.csv"
    $outputFilePath = Join-Path -Path $outputDirectory -ChildPath $outputFileName

    # Import the CSV file
    #$csvData = Import-Csv -Path $inputFilePath

    # Initialize an array to hold filtered records
    $filteredData = @()

    Import-Csv -Path $inputFilePath -ReadCount 1000 | ForEach-Object {
        foreach ($record in $_) {
            # Increment the record count
            $recordCount++

            # Check if the product_or_service_code is in the HashSet
            if ($codesHashSet.Contains($record.product_or_service_code)) {
                $filteredData += $record
            }

            # Output the record number for every 1000 records processed
            if ($recordCount % 1000 -eq 0) {
                Write-Host "Processed $recordCount records from $($csvFile.Name)"
            }
        }
    }

    # Filter the records based on the Product or Service Code
    $filteredData = $csvData | Where-Object { $codesToFilter -contains $_.product_or_service_code }

    # Check if any records were found
    if ($filteredData.Count -eq 0) {
        Write-Host "No records found in $($csvFile.Name) matching the specified product_or_service_code values."
    } else {
        # Export the filtered records to a new CSV file
        $filteredData | Export-Csv -Path $outputFilePath -NoTypeInformation
        Write-Host "Filtered records saved to: $outputFilePath"
        Write-Host "Number of records saved: $($filteredData.Count)"
    }
    # Stop timing the processing
    $endTime = Get-Date
    $duration = $endTime - $startTime

    # Output the processing time
    Write-Host "Processing time for $($csvFile.Name): $($duration.TotalSeconds) seconds"
    
}