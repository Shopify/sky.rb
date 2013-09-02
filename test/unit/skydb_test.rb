require 'test_helper'

class TestSkyDB < MiniTest::Unit::TestCase
  ######################################
  # Denormalization
  ######################################

  def test_denormalize_simple
    data = JSON.parse(IO.read(fixture("skydb/denormalize_simple.json")))
    assert_equal(
      [
        {"gender" => "male", "count" => 1, "sum" => 2},
        {"gender" => "female", "count" => 3, "sum" => 4},
      ],
      SkyDB.denormalize(data, ["gender"])
    )
  end

  def test_denormalize_deep
    data = JSON.parse(IO.read(fixture("skydb/denormalize_deep.json")))
    assert_equal(
      [
        {"count"=>1, "sum"=>2, "gender"=>"male", "state"=>"CO", "country"=>"US"},
        {"count"=>3, "sum"=>4, "gender"=>"female", "state"=>"CO", "country"=>"US"},
        {"count"=>5, "sum"=>6, "gender"=>"male", "state"=>"CA", "country"=>"US"},
        {"count"=>7, "sum"=>8, "gender"=>"female", "state"=>"CA", "country"=>"US"}
      ],
      SkyDB.denormalize(data, ["country", "state", "gender"])
    )
  end


  ######################################
  # Normalization
  ######################################

  def test_normalize_simple
    data = JSON.parse(IO.read(fixture("skydb/normalize.json")))
    assert_equal(
      [
        {"gender"=>"male", "state"=>{"CO"=>{"count"=>1, "sum"=>2}, "CA"=>{"count"=>3, "sum"=>4}}},
        {"gender"=>"female", "state"=>{"CO"=>{"count"=>5, "sum"=>6}, "CA"=>{"count"=>7, "sum"=>8}}}
      ],
      SkyDB.normalize(data, ["state"], ["count", "sum"])
    )
  end

  def test_normalize_deep
    data = JSON.parse(IO.read(fixture("skydb/normalize.json")))
    assert_equal(
      [
        {
          "gender"=>{
            "male"=>{
              "state"=>{
                "CO"=>{"count"=>1, "sum"=>2},
                "CA"=>{"count"=>3, "sum"=>4}
              }
            },
            "female"=>{
              "state"=>{
                "CO"=>{"count"=>5, "sum"=>6},
                "CA"=>{"count"=>7, "sum"=>8}
              }
            }
          }
        }
      ],
      SkyDB.normalize(data, ["gender", "state"], ["count", "sum"])
    )
  end
end
